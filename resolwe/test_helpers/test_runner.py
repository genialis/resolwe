""".. Ignore pydocstyle D400.

===================
Resolwe Test Runner
===================

"""

import ast
import asyncio
import contextlib
import errno
import logging
import os
import re
import shutil
import subprocess
import sys
from functools import partial
from pathlib import Path
from typing import NamedTuple
from unittest.mock import patch

import yaml
import zmq
import zmq.asyncio
from channels.db import database_sync_to_async
from django.conf import settings
from django.core.management.base import CommandError
from django.test import override_settings
from django.test.runner import DiscoverRunner, ParallelTestSuite, RemoteTestRunner
from django.utils.crypto import get_random_string

# Make sure we already have the patched FLOW_* available here; otherwise
# resolwe.test.testcases.TransactionTestCase will override them with the module above,
# negating anything we do here with Django's override_settings.
import resolwe.test.testcases.setting_overrides as resolwe_settings
from resolwe.flow.finders import get_finders
from resolwe.flow.management.commands.prepare_runtime import Command as PrepareRuntime
from resolwe.flow.managers import listener, manager, state
from resolwe.flow.managers.listener.authenticator import ZMQAuthenticator
from resolwe.flow.managers.listener.listener import (
    LISTENER_PRIVATE_KEY,
    LISTENER_PUBLIC_KEY,
)
from resolwe.observers.consumers import update_constants as update_observer_constants
from resolwe.observers.utils import background_task_manager
from resolwe.process.parser import ProcessVisitor
from resolwe.storage.connectors import connectors
from resolwe.test.utils import generate_process_tag

from . import TESTING_CONTEXT

auth = None

logger = logging.getLogger(__name__)

SPAWN_PROCESS_REGEX = re.compile(
    r'run\s+\{.*?["\']process["\']\s*:\s*["\'](.+?)["\'].*?\}'
)


class ZMQInfo(NamedTuple):
    context: zmq.asyncio.Context
    socket: zmq.asyncio.Socket
    authenticator: ZMQAuthenticator


class TestingContext:
    """Context manager which maintains current testing status."""

    def __enter__(self):
        """Enter testing context."""
        TESTING_CONTEXT["is_testing"] = True

    def __exit__(self, *args, **kwargs):
        """Exit testing context."""
        TESTING_CONTEXT["is_testing"] = False

        # Propagate exceptions.
        return False


async def _manager_setup():
    """Execute setup operations common to serial and parallel testing.

    This mostly means state cleanup, such as resetting database
    connections and clearing the shared state.
    """
    if TESTING_CONTEXT.get("manager_reset", False):
        return
    TESTING_CONTEXT["manager_reset"] = True
    state.update_constants()
    update_observer_constants()
    await manager.drain_messages()


def _sequence_paths(paths):
    """Extend the last components of the given paths with a number.

    The method finds the lowest number such that all given paths, when
    extended by it, are unique and can be created. The paths are then
    also created.

    :param paths: The list of paths to be extended and created.
    :return: The list of created paths.
    """
    seq = 0
    while True:
        # Note for parallel execution: infinite zigzagging ladders are
        # not possible, because the directories are always created in
        # the same order. The problem would be if process A succeeded
        # in creating data/test_1, but process B would beat it to
        # upload/test_1 (so that both would roll back and continue
        # with _2, etc.). For B to succeed in creating upload/test_1,
        # it must have already succeeded in creating data/test_1,
        # meaning A could not possibly have succeeded with data/test_1.
        seq += 1
        created = []
        hashseed = os.environ.get("PYTHONHASHSEED", "")

        for base_path in paths:
            path = os.path.join(base_path, "test_{}_{}".format(hashseed, seq))
            try:
                os.makedirs(path)
                created.append(path)
            except OSError as e:
                # Retry on file exists error.
                if e.errno == errno.EEXIST:
                    break
                raise

        if len(created) == len(paths):
            return created

        # If they're not equal, we failed and need to roll back;
        # errors are entirely irrelevant here, removal is purely
        # best effort.
        for path in created:
            try:
                os.rmdir(path)
            except Exception:
                pass


def _create_test_dirs():
    """Create all the testing directories."""
    if "test_paths" in TESTING_CONTEXT:
        return TESTING_CONTEXT["test_paths"]

    # Set the path values to the connectors and volumes. First recreate them from the
    # settings to avoid nested testing folders.
    resolwe_settings.prepare_storage_settings()

    # Create the testing directories and set them to the connectors.
    items = [
        resolwe_settings.STORAGE_CONNECTORS[connector_name]
        for connector_name in resolwe_settings.STORAGE_CONNECTORS
        if connectors[connector_name].mountable
    ]
    items += [
        volume
        for volume in resolwe_settings.FLOW_VOLUMES.values()
        if "read_only" not in volume["config"]
        and volume["type"] != "temporary_directory"
    ]
    paths = _sequence_paths([item["config"]["path"] for item in items])
    for item, path in zip(items, paths):
        item["config"]["path"] = path
    TESTING_CONTEXT["test_paths"] = paths
    return paths


def _prepare_settings():
    """Prepare and apply settings/port overrides needed for testing.

    Override necessary settings and binds to a free port that will be used in
    listener.

    :returns: tuple (overrides, port).
    """
    # Override container name prefix setting.
    mountable_data_connectors = [
        connector for connector in connectors.for_storage("data") if connector.mountable
    ]
    resolwe_settings.FLOW_EXECUTOR_SETTINGS["CONTAINER_NAME_PREFIX"] = (
        "{}_{}_{}".format(
            getattr(settings, "FLOW_EXECUTOR", {}).get(
                "CONTAINER_NAME_PREFIX", "resolwe"
            ),
            # NOTE: This is necessary to avoid container name clashes when tests are run from
            # different Resolwe code bases on the same system (e.g. on a CI server).
            get_random_string(length=6),
            os.path.basename(mountable_data_connectors[0].path),
        )
    )

    hosts = list(
        settings.FLOW_EXECUTOR.get("LISTENER_CONNECTION", {})
        .get("hosts", {"local": "127.0.0.1"})
        .values()
    )
    protocol = settings.FLOW_EXECUTOR.get("LISTENER_CONNECTION", {}).get(
        "protocol", "tcp"
    )
    min_port = settings.FLOW_EXECUTOR.get("LISTENER_CONNECTION", {}).get(
        "min_port", 50000
    )
    max_port = settings.FLOW_EXECUTOR.get("LISTENER_CONNECTION", {}).get(
        "max_port", 60000
    )

    zmq_context: zmq.asyncio.Context = zmq.asyncio.Context.instance()
    zmq_socket: zmq.asyncio.Socket = zmq_context.socket(zmq.ROUTER)
    zmq_socket.setsockopt(zmq.ROUTER_HANDOVER, 1)
    auth = ZMQAuthenticator.instance(zmq_context)
    zmq_socket.curve_secretkey = LISTENER_PRIVATE_KEY
    zmq_socket.curve_publickey = LISTENER_PUBLIC_KEY
    zmq_socket.curve_server = True
    host = hosts[0]
    port = zmq_socket.bind_to_random_port(
        f"{protocol}://{host}", min_port=min_port, max_port=max_port
    )
    for host in hosts[1:]:
        zmq_socket.bind(f"{protocol}://{host}:{port}")

    # Set the port in the settings.
    resolwe_settings.FLOW_EXECUTOR_SETTINGS["LISTENER_CONNECTION"]["port"] = port

    overrides = override_settings(
        CELERY_ALWAYS_EAGER=True,
        FLOW_EXECUTOR=resolwe_settings.FLOW_EXECUTOR_SETTINGS,
        FLOW_MANAGER=resolwe_settings.FLOW_MANAGER_SETTINGS,
    )
    zmq_info = ZMQInfo(zmq_context, zmq_socket, auth)
    return (overrides, zmq_info)


def _custom_worker_init(django_init_worker):
    """Wrap the original worker init to also start the manager."""

    def _init_worker(*args, **kwargs):
        """Initialize a :class:`multiprocessing.Pool` worker.

        Call the Django's ``ParallelTestSuite.init_worker`` and then
        also start the manager infrastructure.
        """
        result = django_init_worker(*args, **kwargs)

        # Further patch channel names and the like with our current pid,
        # so that parallel managers and executors don't clash on the
        # same channels and directories.
        resolwe_settings.FLOW_MANAGER_SETTINGS[
            "REDIS_PREFIX"
        ] += "-parallel-pid{}".format(os.getpid())

        return result

    return _init_worker


def _run_in_event_loop(coro, *args, **kwargs):
    """Run a coroutine in a runloop call.

    This is needed as the top level call into Resolwe Manager-using
    tests. An event loop is started so that it can be used within the
    call tree.

    :param coro: The coroutine to run with an underlying event loop. All
        other arguments given to this function are forwarded to it.
    """
    asyncio.get_event_loop().close()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = asyncio.ensure_future(coro(*args, **kwargs), loop=loop)
    loop.run_until_complete(task)
    loop.close()
    return task.result()


async def _run_on_infrastructure(meth, *args, **kwargs):
    """Start the Manager infrastructure and call the given callable.

    The method given is run through a serializing wrapper, so that
    Django database accesses are correct.

    :param meth: The callable to run on the infrastructure. All other
        arguments are forwarded to it.
    """
    with TestingContext():
        _create_test_dirs()
        overrides, zmq_info = _prepare_settings()
        with overrides:
            with patch.multiple(
                "resolwe.storage.settings",
                STORAGE_CONNECTORS=resolwe_settings.STORAGE_CONNECTORS,
                FLOW_VOLUMES=resolwe_settings.FLOW_VOLUMES,
            ):
                connectors.recreate_connectors()
                await _manager_setup()
                hosts = settings.FLOW_EXECUTOR["LISTENER_CONNECTION"]["hosts"]
                port = settings.FLOW_EXECUTOR["LISTENER_CONNECTION"]["port"]
                protocol = settings.FLOW_EXECUTOR["LISTENER_CONNECTION"]["protocol"]
                listener.hosts = hosts
                listener.port = port
                listener.protocol = protocol
                listener.zmq_socket = zmq_info.socket
                background_task_manager.set_loop(asyncio.get_running_loop())
                async with listener, background_task_manager:
                    try:
                        zmq_info.authenticator.start()
                        zmq_info.authenticator.clear_authorizations()
                        with override_settings(FLOW_MANAGER_SYNC_AUTO_CALLS=True):
                            # Run the test in the new thread instead on the
                            # main thread (default). If test is started on the
                            # main thread then database_sync_to_async calls
                            # will wait indefinitely (they share the same
                            # thread) for the test to finish creating the
                            # infinite loop.
                            result = await database_sync_to_async(
                                meth, thread_sensitive=False
                            )(*args, **kwargs)
                        return result
                    except Exception:
                        logger.exception("Exception while running test")
                    finally:
                        zmq_info.authenticator.stop()
                        # Give authenticator task some time to finish.
                        await asyncio.sleep(0.1)
                        logger.debug("test_runner: Terminating listener")


def _run_manager(meth, *args, **kwargs):
    """Start the Manager properly and nest the given callable in it.

    :param meth: The callable to start the environment for; all other
        arguments are forwarded to it.
    """
    return _run_in_event_loop(_run_on_infrastructure, meth, *args, **kwargs)


class CustomRemoteRunner(RemoteTestRunner):
    """Standard Django remote runner with a custom run method."""

    def run(self, *args, **kwargs):
        """Run the superclass method with an underlying event loop."""
        # NOTE: An instance of this class is constructed for _each_ batch
        # of tests, not just once per pool worker, so some care needs to
        # be taken to avoid incremental corruption, such as in
        # _create_test_dirs().
        return _run_manager(super().run, *args, **kwargs)


class CustomParallelTestSuite(ParallelTestSuite):
    """Standard parallel suite with a custom worker initializer."""

    init_worker = _custom_worker_init(ParallelTestSuite.init_worker)
    runner_class = CustomRemoteRunner


class ResolweRunner(DiscoverRunner):
    """Resolwe test runner."""

    parallel_test_suite = CustomParallelTestSuite

    def __init__(self, *args, **kwargs):
        """Initialize test runner."""
        self.only_changes_to = kwargs.pop("only_changes_to", None)
        self.changes_file_types = kwargs.pop("changes_file_types", None)
        self.keep_data = kwargs.pop("keep_data", False)

        super().__init__(*args, **kwargs)

    @classmethod
    def add_arguments(cls, parser):
        """Add command-line arguments.

        :param parser: Argument parser instance
        """
        super().add_arguments(parser)

        parser.add_argument(
            "--only-changes-to",
            dest="only_changes_to",
            help="Only test changes against given Git commit reference",
        )

        parser.add_argument(
            "--changes-file-types",
            dest="changes_file_types",
            help="File which describes what kind of changes are available",
        )

        parser.add_argument(
            "--keep-data",
            dest="keep_data",
            action="store_true",
            help="Prevent test cases from cleaning up after execution",
        )

    def build_suite(self, *args, **kwargs):
        """Build test suite."""
        suite = super().build_suite(*args, **kwargs)
        # Build suite first constructs the parallel suite and then may reduce self.parallel,
        # while keeping suite.processes unchanged. We need to propagate the change here to
        # avoid spawning more processes than there are databases.
        suite.processes = self.parallel

        # Augment all test cases with manager state validation logic.
        def validate_manager_state(case, teardown):
            """Decorate test case with manager state validation."""

            def wrapper(*args, **kwargs):
                """Validate manager state on teardown."""
                teardown(*args, **kwargs)

            return wrapper

        if isinstance(suite, self.parallel_test_suite):
            # NOTE: validate_manager_state function cannot be pickled, so it
            #       cannot be used in parallel tests.
            pass
        else:
            for case in suite:
                case.tearDown = validate_manager_state(case, case.tearDown)

        return suite

    def run_suite(self, suite, **kwargs):
        """Run the test suite with manager workers in the background."""
        # Due to the way the app modules are imported, there's no way to
        # statically override settings with TEST overrides before e.g.
        # resolwe.flow.managers.manager is loaded somewhere in the code
        # (the most problematic files are signals.py and
        # resolwe.test.testcases.process); the only realistic mechanism
        # is to override later and call some sort of commit method in
        # the manager.
        keep_data_override = override_settings(FLOW_MANAGER_KEEP_DATA=self.keep_data)
        keep_data_override.__enter__()

        # Remove old and prepare new runtime environment.
        # Remove everything except possibly README.rst file in the runtime directory.
        runtime_dir = Path(resolwe_settings.FLOW_VOLUMES["runtime"]["config"]["path"])
        for entry in runtime_dir.glob("*"):
            if entry.name != "README.rst":
                if entry.is_dir():
                    shutil.rmtree(entry, ignore_errors=True)
                else:
                    entry.unlink()
        # Recreate runtime environment.
        PrepareRuntime().prepare_runtime(runtime_dir)

        if self.parallel > 1:
            return super().run_suite(suite, **kwargs)

        return _run_manager(super().run_suite, suite, **kwargs)

    def run_tests(self, test_labels, **kwargs):
        """Run tests.

        :param test_labels: Labels of tests to run
        """
        if self.only_changes_to:
            # Check if there are changed files. We need to be able to switch between branches so we
            # can correctly detect changes.
            repo_status = self._git("status", "--porcelain", "--untracked-files=no")
            if repo_status:
                print(
                    "ERROR: Git repository is not clean. Running tests with --only-changes-to",
                    file=sys.stderr,
                )
                print(
                    "       requires that the current Git repository is clean.",
                    file=sys.stderr,
                )
                return False

            print(
                "Detecting changed files between {} and HEAD.".format(
                    self.only_changes_to
                )
            )
            changed_files = self._git(
                "diff", "--name-only", self.only_changes_to, "HEAD"
            )
            changed_files = changed_files.strip().split("\n")
            changed_files = [file for file in changed_files if file.strip()]

            top_level_path = self._git("rev-parse", "--show-toplevel")
            top_level_path = top_level_path.strip()

            # Process changed files to discover what they are.
            changed_files, tags, tests, full_suite = self.process_changed_files(
                changed_files, top_level_path
            )
            print("Changed files:")
            for filename, file_type in changed_files:
                print("  {} ({})".format(filename, file_type))

            if not changed_files:
                print("  none")
                print(
                    "No files have been changed, assuming target is HEAD, running full suite."
                )
            elif full_suite:
                print(
                    "Non-test code or unknown files have been modified, running full test suite."
                )
            else:
                # Run specific tests/tags.
                print("Running with following partial tags: {}".format(", ".join(tags)))
                print(
                    "Running with following partial tests: {}".format(", ".join(tests))
                )

                failed_tests = 0

                # First run with specific tags. Since run_tests may modify self.parallel, we need to store
                # it here and restore it later if we also run with specific test labels.
                parallel = self.parallel
                if tags:
                    self.tags = tags
                    failed_tests += super().run_tests(test_labels, **kwargs)

                # Then run with specific test labels.
                if tests:
                    # Reset manager before another run.
                    TESTING_CONTEXT.pop("manager_reset", None)
                    TESTING_CONTEXT.pop("test_paths", None)
                    self.parallel = parallel
                    self.tags = set()
                    # Exclude tests with the given tags as they ran in the previous step.
                    self.exclude_tags = tags

                    failed_tests += super().run_tests(tests, **kwargs)

                return failed_tests

        return super().run_tests(test_labels, **kwargs)

    def _git(self, *args):
        """Helper to run Git command."""
        try:
            return subprocess.check_output(["git"] + list(args)).decode("utf8").strip()
        except subprocess.CalledProcessError:
            raise CommandError("Git command failed.")

    @contextlib.contextmanager
    def git_branch(self, branch):
        """Temporarily switch to a different Git branch."""
        current_branch = self._git("rev-parse", "--abbrev-ref", "HEAD")
        if current_branch == "HEAD":
            # Detached HEAD state, we need to get the actual commit.
            current_branch = self._git("rev-parse", "HEAD")

        if current_branch != branch:
            self._git("checkout", branch)

        try:
            yield
        finally:
            if current_branch != branch:
                self._git("checkout", current_branch)

    def process_changed_files(self, changed_files, top_level_path):
        """Process changed files based on specified patterns.

        :param changed_files: A list of changed file pats, relative to top-level path
        :param top_level_path: Absolute path to top-level project directory
        :return: Tuple (changed_files, tags, tests, full_suite)
        """
        result = []
        processes = []
        tests = []
        full_suite = False
        types = []

        if self.changes_file_types:
            # Parse file types metadata.
            try:
                with open(self.changes_file_types, "r") as definition_file:
                    types = yaml.load(definition_file, Loader=yaml.FullLoader)
            except (OSError, ValueError):
                raise CommandError("Failed loading or parsing file types metadata.")
        else:
            print(
                "WARNING: Treating all files as unknown because --changes-file-types option not specified.",
                file=sys.stderr,
            )

        for filename in changed_files:
            # Match file type.
            file_type = "unknown"
            file_type_name = "unknown"

            for definition in types:
                if re.search(definition["match"], filename):
                    file_type = definition["type"]
                    file_type_name = definition.get("name", file_type)
                    break

            result.append((filename, file_type_name))
            if file_type in ("unknown", "force_run"):
                full_suite = True
            elif file_type == "ignore":
                # Ignore
                pass
            elif file_type in ("process", "python_process"):
                # Add filename to the process list.
                processes.append(os.path.join(top_level_path, filename))
            elif file_type == "test":
                # Generate test name.
                tests.append(re.sub(r"\.py$", "", filename).replace(os.path.sep, "."))
            else:
                raise CommandError("Unsupported file type: {}".format(file_type))

        # Resolve process tags. The resolwe_process_tags raises RuntimeError
        # when dependencies can not be determined. The entire test suite should
        # bemrun in such case.
        try:
            tags = self.resolve_process_tags(processes)
        except RuntimeError as e:
            print(f"Error while resolving process tags: '{e}'.")
            full_suite = True
            tags = []
        return result, tags, tests, full_suite

    def find_schemas(self, schema_path):
        """Find process schemas.

        :param schema_path: Path where to look for process schemas
        :return: Found schemas
        """
        schema_loader = {
            "python": lambda file_handler: [file_handler.read()],
            "yaml": partial(yaml.load, Loader=yaml.FullLoader),
        }
        changed_schemas = {"python": [], "yaml": []}
        schema_file_suffixes = {"python": (".py",), "yaml": (".yml", ".yaml")}

        for root, _, files in os.walk(schema_path):
            for schema_file in [os.path.join(root, fn) for fn in files]:
                # Check for processes of both types: python and yaml.
                for schema_type in schema_loader.keys():
                    if not schema_file.lower().endswith(
                        schema_file_suffixes[schema_type]
                    ):
                        continue

                    with open(schema_file) as file_handler:
                        schemas = schema_loader[schema_type](file_handler)

                    if not schemas:
                        print(
                            "WARNING: Could not read {}".format(schema_file),
                            file=sys.stderr,
                        )
                        continue

                    for schema in schemas:
                        changed_schemas[schema_type].append(schema)

        return changed_schemas

    def find_python_dependencies(self, source):
        """Find slugs of the processes that the given source can execute.

        :raises RuntimeError: when dependencies could not be determines.
        """
        root = ast.parse(source)
        visitor = ProcessVisitor(source=source)
        visitor.visit(root)
        return visitor.get_dependencies()

    def find_dependencies(self, schemas):
        """Compute process dependencies.

        :param schemas: A list of all discovered process schemas
        :raises ValueError: when schema type (keys in schemas dictionary) is unknown.
        :return: Process dependency dictionary. The keys are processes and the values
            are the processes that call them as part of their workflow.
        """
        dependencies = {}
        # Schema type can be "python" or "yaml"
        for schema_type in schemas:
            for schema in schemas[schema_type]:
                if schema_type == "yaml":
                    slug = schema["slug"]
                    run = schema.get("run", {})
                    program = run.get("program", None)
                    language = run.get("language", None)

                    if language == "workflow":
                        for step in program:
                            dependencies.setdefault(step["run"], set()).add(slug)
                    elif language == "bash":
                        # Process re-spawn instructions to discover dependencies.
                        matches = SPAWN_PROCESS_REGEX.findall(program)
                        if matches:
                            for match in matches:
                                dependencies.setdefault(match, set()).add(slug)
                elif schema_type == "python":
                    python_dependencies = self.find_python_dependencies(schema)
                    if python_dependencies:
                        for (
                            process_slug,
                            called_processes,
                        ) in python_dependencies.items():
                            for called_process in called_processes:
                                dependencies.setdefault(called_process, set()).add(
                                    process_slug
                                )
                else:
                    raise ValueError(
                        f"Unknown schema type {schema_type}. It has to be one of 'python' or 'yaml'."
                    )
        return dependencies

    def resolve_process_tags(self, files):
        """Resolve process tags for yaml and Python processes.

        :param files: List of changed process files
        :return: Test tags that need to be run
        """
        processes_paths = []
        for finder in get_finders():
            processes_paths.extend(finder.find_processes())

        process_schemas = {"python": [], "yaml": []}
        for proc_path in processes_paths:
            schemas = self.find_schemas(proc_path)
            for schema_type in schemas:
                process_schemas[schema_type].extend(schemas[schema_type])

        # Switch to source branch and get all the schemas from there as well, since some schemas
        # might have been removed.
        with self.git_branch(self.only_changes_to):
            for proc_path in processes_paths:
                schemas = self.find_schemas(proc_path)
                for schema_type in schemas:
                    process_schemas[schema_type].extend(schemas[schema_type])

        dependencies = self.find_dependencies(process_schemas)
        processes = set()

        def load_process_slugs(filename):
            """Add all process slugs from specified file."""
            with open(filename, "r") as process_file:
                if filename.endswith(".py"):
                    source = process_file.read()
                    root = ast.parse(source)
                    visitor = ProcessVisitor(source=source)
                    visitor.visit(root)
                    for process in visitor.processes:
                        processes.add(process.metadata.slug)
                else:
                    data = yaml.load(process_file, Loader=yaml.FullLoader)
                    for process in data:
                        # Add all process slugs.
                        processes.add(process["slug"])

        for filename in files:
            try:
                load_process_slugs(filename)
            except FileNotFoundError:
                # File was removed, so we will handle it below when we check the original branch.
                pass

        # Switch to source branch and check modified files there as well.
        with self.git_branch(self.only_changes_to):
            for filename in files:
                try:
                    load_process_slugs(filename)
                except FileNotFoundError:
                    # File was added, so it has already been handled.
                    pass

        # Add all dependencies.
        dep_processes = set()
        while processes:
            process = processes.pop()
            if process in dep_processes:
                continue

            dep_processes.add(process)
            processes.update(dependencies.get(process, set()))

        tags = set()
        for process in dep_processes:
            tags.add(generate_process_tag(process))

        return tags
