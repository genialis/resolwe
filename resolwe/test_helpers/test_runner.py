""".. Ignore pydocstyle D400.

===================
Resolwe Test Runner
===================

"""
import contextlib
import logging
import os
import re
import shutil
import subprocess
import sys
# Parallel Django test execution is done with worker pools from the multiprocessing module;
# since every worker process needs its own initialization code to start the manager infrastructure,
# it also needs a teardown function, but the multiprocessing module fails to provide such
# functionality publicly. Internally, the Finalize class imported below is used.
from multiprocessing.util import Finalize  # undocumented
from signal import SIGINT, SIGKILL

import mock
import yaml

from django import db
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import override_settings
from django.test.runner import DiscoverRunner, ParallelTestSuite
from django.utils.crypto import get_random_string

# Make sure we already have the patched FLOW_* available here; otherwise
# resolwe.test.testcases.TransactionTestCase will override them with the module above,
# negating anything we do here with Django's override_settings.
import resolwe.test.testcases.setting_overrides as resolwe_settings
from resolwe.flow.finders import get_finders
from resolwe.flow.managers import manager, state
from resolwe.test.utils import generate_process_tag
from resolwe.utils import BraceMessage as __

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name

SPAWN_PROCESS_REGEX = re.compile(r'run\s+\{.*?["\']process["\']\s*:\s*["\'](.+?)["\'].*?\}')

TESTING_CONTEXT = {
    'is_testing': False,
}


class TestingContext(object):
    """Context manager which maintains current testing status."""

    def __enter__(self):
        """Enter testing context."""
        TESTING_CONTEXT['is_testing'] = True

    def __exit__(self, *args, **kwargs):
        """Exit testing context."""
        TESTING_CONTEXT['is_testing'] = False

        # Propagate exceptions.
        return False


class CommandContext(object):
    """Async wrapper around Django management commands.

    This needs to be done with standalone processes instead of threads,
    because both ``runworker`` and ``runlistener`` install signal
    handlers which, in Python, can only be done on the main thread.
    """

    def __init__(self, command, *args, **kwargs):
        """Initialize instance variables.

        :param command: The Django command to be run.
        :param args: Positional arguments passed to the command.
        :param kwargs: Keyword arguments passed to the command.
        """
        self._django_command = command
        self._args = args
        self._kwargs = kwargs
        self._pid = None
        super().__init__()

    def __enter__(self):
        """Set up a context manager for the specified Django command.

        The command is executed in a separate process which inherits the
        Python context from the current one.
        """
        # In case of parallel execution, this code will be run from a multiprocessing.Pool worker.
        # Because those are all daemonic, we can't use the normal multiprocessing.Process chrome;
        # the remaining alternative is plain posix forking, since spawning a process by image name
        # would be even more messy due to the difficulty in divining the way this process was started.
        pid = os.fork()
        if pid == 0:
            try:
                self.run()
            except Exception:  # pylint: disable=broad-except
                logger.error(
                    __("Resolwe test runner: command '{}' crashed.", self._django_command),
                    exc_info=True
                )
            finally:
                # If sys.exit(0) is used here, Django's testing framework will stupidly catch it
                # as if it was an error, so just commit suicide as quickly as possible. Returning
                # from the function would lead to chaos and madness because Django isn't aware that
                # we've forked; cleanup handlers would get called twice (from here and from the parent).
                os.kill(os.getpid(), SIGKILL)
        else:
            self._pid = pid
            return self

    def __exit__(self, *args, **kwargs):
        """On exiting a context, kill the command and wait for it."""
        os.kill(self._pid, SIGINT)
        os.waitpid(self._pid, 0)
        # Propagate exceptions.
        return False

    def run(self):
        """Run the Django command specified in the constructor."""
        call_command(self._django_command, *self._args, **self._kwargs)


class AtScopeExit(object):
    """Utility class for calling a function once a context exits."""

    def __init__(self, call, *args, **kwargs):
        """Construct a context manager and save arguments.

        :param call: The callable to call on exit.
        :param args: Positional arguments for the callable.
        :param kwargs: Keyword arguments for the callable.
        """
        self.call = call
        self.args = args
        self.kwargs = kwargs

    def __enter__(self):
        """Enter the ``with`` context."""
        return self

    def __exit__(self, *args, **kwargs):
        """Exit the context and call the saved callable."""
        self.call(*self.args, **self.kwargs)
        return False


def _manager_setup():
    """Execute setup operations common to serial and parallel testing.

    This mostly means state cleanup, such as resetting database
    connections and clearing the shared state.
    """
    for alias in db.connections:
        conn = db.connections[alias]
        conn.close()
        # Make very sure the connection is actually closed here, or the
        # same descriptor might be used in the processes we fork off in
        # the runner. In particular, the django_db_geventpool pools have
        # a closeall() method which isn't a no-op and actually shuts
        # down the pool.
        if hasattr(conn, 'closeall'):
            conn.closeall()
    state.update_constants()
    manager.reset()


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

        for base_path in paths:
            path = os.path.join(base_path, 'test_{}'.format(seq))
            try:
                os.makedirs(path)
                created.append(path)
            except OSError:
                break

        if len(created) == len(paths):
            return created

        # If they're not equal, we failed and need to roll back;
        # errors are entirely irrelevant here, removal is purely
        # best effort.
        for path in created:
            try:
                os.rmdir(path)
            except:  # pylint: disable=bare-except
                pass


def _create_test_dirs():
    """Create all the testing directories."""
    items = ['DATA_DIR', 'UPLOAD_DIR', 'RUNTIME_DIR']
    paths = _sequence_paths([resolwe_settings.FLOW_EXECUTOR_SETTINGS[i] for i in items])
    for item, path in zip(items, paths):
        resolwe_settings.FLOW_EXECUTOR_SETTINGS[item] = path
    return paths


def _prepare_settings():
    """Prepare and apply settings overrides needed for testing."""
    # Override container name prefix setting.
    resolwe_settings.FLOW_EXECUTOR_SETTINGS['CONTAINER_NAME_PREFIX'] = '{}_{}_{}'.format(
        resolwe_settings.FLOW_EXECUTOR_SETTINGS.get('CONTAINER_NAME_PREFIX', 'resolwe'),
        # NOTE: This is necessary to avoid container name clashes when tests are run from
        # different Resolwe code bases on the same system (e.g. on a CI server).
        get_random_string(length=6),
        os.path.basename(resolwe_settings.FLOW_EXECUTOR_SETTINGS['DATA_DIR'])
    )

    return override_settings(
        CELERY_ALWAYS_EAGER=True,
        FLOW_EXECUTOR=resolwe_settings.FLOW_EXECUTOR_SETTINGS,
        FLOW_MANAGER=resolwe_settings.FLOW_MANAGER_SETTINGS,
    )


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
        resolwe_settings.FLOW_MANAGER_SETTINGS['REDIS_PREFIX'] += '-parallel-pid{}'.format(os.getpid())

        testing = TestingContext()
        testing.__enter__()
        Finalize(testing, testing.__exit__, exitpriority=16)

        dirs = _create_test_dirs()

        try:
            overrides = _prepare_settings()
            overrides.__enter__()
            Finalize(overrides, lambda: overrides.__exit__(None, None, None), exitpriority=16)

            _manager_setup()

            state_cleanup = AtScopeExit(manager.state.destroy_channels)
            state_cleanup.__enter__()
            Finalize(state_cleanup, state_cleanup.__exit__, exitpriority=16)

            listener = CommandContext('runlistener', '--clear-queue')
            listener.__enter__()
            Finalize(listener, listener.__exit__, exitpriority=16)

            workers = CommandContext('runworker', only_channels=[state.MANAGER_CONTROL_CHANNEL])
            workers.__enter__()
            Finalize(workers, workers.__exit__, exitpriority=16)

            signal_override = override_settings(FLOW_MANAGER_SYNC_AUTO_CALLS=True)
            signal_override.__enter__()
            Finalize(signal_override, lambda: signal_override.__exit__(None, None, None), exitpriority=16)

            return result
        except:  # pylint: disable=bare-except
            # There's nothing we can do at this point, init _must_ succeed or the pool will try
            # restarting us on every pool action from the suite runner, leading to an
            # infinite loop and, to the outside, an apparent hang.
            #
            # Code after us will almost certainly also fail, which should lead to orderly
            # test failure and eventually suite shutdown once all tests are through.
            #
            # The best we can do here is make sure we don't leave stale directories behind,
            # which as a side effect also makes it more likely that testing will fail early.
            logger.exception("An exception occurred during early parallel worker initialization.")
            for path in dirs:
                try:
                    os.rmdir(path)
                except:  # pylint: disable=bare-except
                    pass
    return _init_worker


class CustomParallelTestSuite(ParallelTestSuite):
    """Standard parallel suite with a custom worker initializer."""

    init_worker = _custom_worker_init(ParallelTestSuite.init_worker)


class ResolweRunner(DiscoverRunner):
    """Resolwe test runner."""

    parallel_test_suite = CustomParallelTestSuite

    def __init__(self, *args, **kwargs):
        """Initialize test runner."""
        self.only_changes_to = kwargs.pop('only_changes_to', None)
        self.changes_file_types = kwargs.pop('changes_file_types', None)

        # Handle implication first, meanings get inverted later.
        self.keep_data = False
        if 'no_mock_purge' in kwargs:
            self.keep_data = True

        # mock_purge used to be the default, so keep it that way;
        # this means the command line option does the opposite
        # (disables it), so the boolean must be inverted here.
        self.mock_purge = not kwargs.pop('no_mock_purge', False)

        self.keep_data = kwargs.pop('keep_data', self.keep_data)

        super().__init__(*args, **kwargs)

    @classmethod
    def add_arguments(cls, parser):
        """Add command-line arguments.

        :param parser: Argument parser instance
        """
        super().add_arguments(parser)

        parser.add_argument(
            '--only-changes-to', dest='only_changes_to',
            help="Only test changes against given Git commit reference"
        )

        parser.add_argument(
            '--changes-file-types', dest='changes_file_types',
            help="File which describes what kind of changes are available"
        )

        parser.add_argument(
            '--keep-data', dest='keep_data', action='store_true',
            help="Prevent test cases from cleaning up after execution"
        )

        parser.add_argument(
            '--no-mock-purge', dest='no_mock_purge', action='store_true',
            help="Do not mock purging functions (implies --keep-data)"
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
                if int(manager.state.executor_count) != 0 or int(manager.state.sync_semaphore) != 0:
                    case.fail(
                        'Test has outstanding manager processes. Ensure that all processes have '
                        'completed or that you have reset the state manually in case you have '
                        'bypassed the regular manager flow in any way.\n'
                        '\n'
                        'Executor count: {executor_count} (should be 0)\n'
                        'Sync semaphore: {sync_semaphore} (should be 0)\n'
                        ''.format(
                            executor_count=int(manager.state.executor_count),
                            sync_semaphore=int(manager.state.sync_semaphore),
                        )
                    )

                teardown(*args, **kwargs)

            return wrapper

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

        if self.keep_data and self.mock_purge:
            purge_mock_os = mock.patch('resolwe.flow.utils.purge.os', wraps=os).start()
            purge_mock_os.remove = mock.MagicMock()

            purge_mock_shutil = mock.patch('resolwe.flow.utils.purge.shutil', wraps=shutil).start()
            purge_mock_shutil.rmtree = mock.MagicMock()

        if self.parallel > 1:
            return super().run_suite(suite, **kwargs)

        with TestingContext():
            _create_test_dirs()
            with _prepare_settings():
                _manager_setup()
                with AtScopeExit(manager.state.destroy_channels):
                    with CommandContext('runlistener', '--clear-queue'):
                        with CommandContext('runworker', only_channels=[state.MANAGER_CONTROL_CHANNEL]):
                            with override_settings(FLOW_MANAGER_SYNC_AUTO_CALLS=True):
                                return super().run_suite(suite, **kwargs)

    def run_tests(self, test_labels, **kwargs):
        """Run tests.

        :param test_labels: Labels of tests to run
        """
        if self.only_changes_to:
            # Check if there are changed files. We need to be able to switch between branches so we
            # can correctly detect changes.
            repo_status = self._git('status', '--porcelain', '--untracked-files=no')
            if repo_status:
                print("ERROR: Git repository is not clean. Running tests with --only-changes-to", file=sys.stderr)
                print("       requires that the current Git repository is clean.", file=sys.stderr)
                return False

            print("Detecting changed files between {} and HEAD.".format(self.only_changes_to))
            changed_files = self._git('diff', '--name-only', self.only_changes_to, 'HEAD')
            changed_files = changed_files.strip().split('\n')
            changed_files = [file for file in changed_files if file.strip()]

            top_level_path = self._git('rev-parse', '--show-toplevel')
            top_level_path = top_level_path.strip()

            # Process changed files to discover what they are.
            changed_files, tags, tests, full_suite = self.process_changed_files(changed_files, top_level_path)
            print("Changed files:")
            for filename, file_type in changed_files:
                print("  {} ({})".format(filename, file_type))

            if not changed_files:
                print("  none")
                print("No files have been changed, assuming target is HEAD, running full suite.")
            elif full_suite:
                print("Non-test code or unknown files have been modified, running full test suite.")
            else:
                # Run specific tests/tags.
                print("Running with following partial tags: {}".format(', '.join(tags)))
                print("Running with following partial tests: {}".format(', '.join(tests)))

                failed_tests = 0

                # First run with specific tags. Since run_tests may modify self.parallel, we need to store
                # it here and restore it later if we also run with specific test labels.
                parallel = self.parallel
                if tags:
                    self.tags = tags
                    failed_tests += super().run_tests(test_labels, **kwargs)

                # Then run with specific test labels.
                if tests:
                    self.parallel = parallel
                    self.tags = set()
                    failed_tests += super().run_tests(tests, **kwargs)

                return failed_tests

        return super().run_tests(test_labels, **kwargs)

    def _git(self, *args):
        """Helper to run Git command."""
        try:
            return subprocess.check_output(['git'] + list(args)).decode('utf8').strip()
        except subprocess.CalledProcessError:
            raise CommandError("Git command failed.")

    @contextlib.contextmanager
    def git_branch(self, branch):
        """Temporarily switch to a different Git branch."""
        current_branch = self._git('rev-parse', '--abbrev-ref', 'HEAD')
        if current_branch == 'HEAD':
            # Detached HEAD state, we need to get the actual commit.
            current_branch = self._git('rev-parse', 'HEAD')

        if current_branch != branch:
            self._git('checkout', branch)

        try:
            yield
        finally:
            if current_branch != branch:
                self._git('checkout', current_branch)

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
                with open(self.changes_file_types, 'r') as definition_file:
                    types = yaml.load(definition_file)
            except (OSError, ValueError):
                raise CommandError("Failed loading or parsing file types metadata.")
        else:
            print("WARNING: Treating all files as unknown because --changes-file-types option not specified.",
                  file=sys.stderr)

        for filename in changed_files:
            # Match file type.
            file_type = 'unknown'
            file_type_name = 'unknown'

            for definition in types:
                if re.search(definition['match'], filename):
                    file_type = definition['type']
                    file_type_name = definition.get('name', file_type)
                    break

            result.append((filename, file_type_name))
            if file_type in ('unknown', 'force_run'):
                full_suite = True
            elif file_type == 'ignore':
                # Ignore
                pass
            elif file_type == 'process':
                # Resolve process tag.
                processes.append(os.path.join(top_level_path, filename))
            elif file_type == 'test':
                # Generate test name.
                tests.append(re.sub(r'\.py$', '', filename).replace(os.path.sep, '.'))
            else:
                raise CommandError("Unsupported file type: {}".format(file_type))

        # Resolve tags.
        tags = self.resolve_process_tags(processes)

        return result, tags, tests, full_suite

    def find_schemas(self, schema_path):
        """Find process schemas.

        :param schema_path: Path where to look for process schemas
        :return: Found schemas
        """
        schema_matches = []

        for root, _, files in os.walk(schema_path):
            for schema_file in [os.path.join(root, fn) for fn in files]:

                if not schema_file.lower().endswith(('.yml', '.yaml')):
                    continue

                with open(schema_file) as fn:
                    schemas = yaml.load(fn)

                if not schemas:
                    print("WARNING: Could not read YAML file {}".format(schema_file), file=sys.stderr)
                    continue

                for schema in schemas:
                    schema_matches.append(schema)

        return schema_matches

    def find_dependencies(self, schemas):
        """Compute process dependencies.

        :param schemas: A list of all discovered process schemas
        :return: Process dependency dictionary
        """
        dependencies = {}

        for schema in schemas:
            slug = schema['slug']
            run = schema.get('run', {})
            program = run.get('program', None)
            language = run.get('language', None)

            if language == 'workflow':
                for step in program:
                    dependencies.setdefault(step['run'], set()).add(slug)
            elif language == 'bash':
                # Process re-spawn instructions to discover dependencies.
                matches = SPAWN_PROCESS_REGEX.findall(program)
                if matches:
                    for match in matches:
                        dependencies.setdefault(match, set()).add(slug)

        return dependencies

    def resolve_process_tags(self, files):
        """Resolve process tags.

        :param files: List of changed process files
        :return: Test tags that need to be run
        """
        processes_paths = []
        for finder in get_finders():
            processes_paths.extend(finder.find_processes())

        process_schemas = []
        for proc_path in processes_paths:
            process_schemas.extend(self.find_schemas(proc_path))

        # Switch to source branch and get all the schemas from there as well, since some schemas
        # might have been removed.
        with self.git_branch(self.only_changes_to):
            for proc_path in processes_paths:
                process_schemas.extend(self.find_schemas(proc_path))

        dependencies = self.find_dependencies(process_schemas)
        processes = set()

        def load_process_slugs(filename):
            """Add all process slugs from specified file."""
            with open(filename, 'r') as process_file:
                data = yaml.load(process_file)

                for process in data:
                    # Add all process slugs.
                    processes.add(process['slug'])

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


def is_testing():
    """Return current testing status."""
    return TESTING_CONTEXT['is_testing']
