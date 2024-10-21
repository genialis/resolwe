""".. Ignore pydocstyle D400.

.. autoclass:: resolwe.test.ProcessTestCase
    :members:

"""

import contextlib
import filecmp
import gzip
import hashlib
import io
import json
import os
import shutil
import sys
import tarfile
import tempfile
import time
import uuid
import zipfile
from itertools import filterfalse

from django.conf import settings
from django.core import management
from django.core.exceptions import ValidationError
from django.core.validators import URLValidator
from django.db import transaction
from django.utils.text import slugify

from resolwe.flow.models import Data, DescriptorSchema, Process, Storage
from resolwe.flow.utils import dict_dot, iterate_fields, iterate_schema
from resolwe.permissions.models import PermissionGroup
from resolwe.storage.connectors import connectors
from resolwe.test import TransactionTestCase

from ..utils import get_processes_from_tags, has_process_tag

SCHEMAS_FIXTURE_CACHE = None


class TestProfiler:
    """Simple test profiler."""

    def __init__(self, test):
        """Initialize test profiler.

        :param test: Unit test instance
        """
        self._test = test
        self._start = time.time()

        if getattr(settings, "TEST_PROCESS_PROFILE", False):
            self._file = open(
                "profile-resolwe-process-tests-{}.json".format(os.getpid()), "a"
            )
        else:
            self._file = None

        # Automatically cleanup when test completes.
        test.addCleanup(self.close)

    def add(self, data):
        """Add output to profile log.

        :param data: Arbitrary data dictionary
        """
        if not self._file:
            return

        data.update({"test": self._test.id()})

        self._file.write(json.dumps(data))
        self._file.write("\n")

    @contextlib.contextmanager
    def add_block(self, name):
        """Profile a named block of code.

        :param name: Block name
        """
        block_start = time.time()
        try:
            yield
        finally:
            block_end = time.time()
            self.add({name: block_end - block_start})

    def close(self):
        """Close profiler log."""
        if not self._file:
            return

        self.add({"total": time.time() - self._start})
        self._file.close()


class ProcessTestCase(TransactionTestCase):
    """Base class for writing process tests.

    It is a subclass of :class:`.TransactionTestCase` with some specific
    functions used for testing processes.

    To write a process test use standard Django's syntax for writing
    tests and follow the next steps:

    #. Put input files (if any) in ``tests/files`` directory of a
       Django application.
    #. Run the process using
       :meth:`.run_process`.
    #. Check if the process has the expected status using
       :meth:`.assertStatus`.
    #. Check process's output using :meth:`.assertFields`,
       :meth:`.assertFile`, :meth:`.assertFileExists`,
       :meth:`.assertFiles` and :meth:`.assertJSON`.

    .. note::
        When creating a test case for a custom Django application,
        subclass this class and over-ride the ``self.files_path`` with:

        .. code-block:: python

            self.files_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files')

    .. DANGER::
        If output files don't exist in ``tests/files`` directory of a
        Django application, they are created automatically.
        But you have to check that they are correct before using them
        for further runs.

    """

    def _update_schema_relations(self, schemas):
        """Update foreign keys on process and descriptor schema.

        The field contributor is updated.

        """
        for schema in schemas:
            schema.contributor = self.admin

    def _create_permission_groups(self, schemas):
        """Create permission groups."""
        for schema in schemas:
            schema.permission_group = PermissionGroup.objects.create()

    def _register_schemas(
        self, path=None, processes_paths=None, descriptors_paths=None
    ):
        """Register process and descriptor schemas.

        If ``processes_paths`` or ``descriptor_path`` arguments are
        given, only processes/descriptors from that path are registered.

        ``path`` argument is supported for backward compatibility and
        applies to both arguments above.

        Process and DescriptorSchema are cached to SCHEMAS_FIXTURE_CACHE
        global variable based on ``processes_paths`` and
        ``descriptors_paths`` keys in ``kwargs``.

        """

        def remove_pks(schemas):
            """Remove primary keys from the given schemas."""
            for s in schemas:
                s.pk = None
            return schemas

        if path:
            if processes_paths or descriptors_paths:
                raise ValueError(
                    "processes_paths and descriptors_paths arguments must be None if path is defined."
                )
            processes_paths = descriptors_paths = path

        schemas_types = [
            {
                "name": "descriptor_schemas",
                "model": DescriptorSchema,
                "cache_key": descriptors_paths,
            },
            {
                "name": "processes",
                "model": Process,
                "cache_key": processes_paths,
            },
        ]

        for schemas in schemas_types:
            schemas["model"].objects.all().delete()

        cache_key = str(
            "processes_paths={};descriptors_paths={}".format(
                processes_paths, descriptors_paths
            )
        )
        global SCHEMAS_FIXTURE_CACHE
        if not SCHEMAS_FIXTURE_CACHE:
            SCHEMAS_FIXTURE_CACHE = {}

        stdout, stderr = io.StringIO(), io.StringIO()

        if cache_key in SCHEMAS_FIXTURE_CACHE:
            for schemas in schemas_types:
                # NOTE: Schemas' current primary keys may not be unique on the next runs of
                # processes' tests, therefore we must remove them and let the DB re-create them
                # properly.
                # WARNING: Cached schemas' primary keys will be set on every call to bulk_create(),
                # therefore we need to remove them and let the DB re-create them every time. For
                # more details, see:
                # https://github.com/django/django/blob/1.10.7/django/db/models/query.py#L455-L456
                schemas_cache = remove_pks(
                    SCHEMAS_FIXTURE_CACHE[cache_key][schemas["name"]]
                )
                self._update_schema_relations(schemas_cache)
                self._create_permission_groups(schemas_cache)
                schemas["model"].objects.bulk_create(schemas_cache)
        else:
            if processes_paths is None and descriptors_paths is None:
                management.call_command(
                    "register", force=True, stdout=stdout, stderr=stderr
                )
            else:
                settings_overrides = {
                    "FLOW_PROCESSES_FINDERS": [
                        "resolwe.flow.finders.FileSystemProcessesFinder"
                    ]
                }
                if processes_paths is not None:
                    settings_overrides["FLOW_PROCESSES_DIRS"] = processes_paths
                if descriptors_paths is not None:
                    settings_overrides["FLOW_DESCRIPTORS_DIRS"] = descriptors_paths

                with self.settings(**settings_overrides):
                    management.call_command(
                        "register", force=True, stdout=stdout, stderr=stderr
                    )

            if cache_key not in SCHEMAS_FIXTURE_CACHE:
                SCHEMAS_FIXTURE_CACHE[cache_key] = {}

            # NOTE: list() forces DB query execution
            for schemas in schemas_types:
                SCHEMAS_FIXTURE_CACHE[cache_key][schemas["name"]] = list(
                    schemas["model"].objects.all()
                )

        return stdout, stderr

    def setUp(self):
        """Initialize test data."""
        super().setUp()

        _, stderr = self._register_schemas()
        stderr = stderr.getvalue()
        if stderr:
            raise RuntimeError(stderr)

        upload_connectors = [
            connector
            for connector in connectors.for_storage("upload")
            if connector.mountable
        ]
        assert upload_connectors, "No upload connector defined on filesystem"

        self.upload_dir = upload_connectors[0].path
        self._profiler = TestProfiler(self)
        self._preparation_stage = 0
        self._executed_processes = set()
        self._files_path = None
        self._upload_files = []

        # create upload dir if it doesn't exist
        if not os.path.isdir(self.upload_dir):
            os.mkdir(self.upload_dir)

    def tearDown(self):
        """Clean up after the test."""
        # delete Data objects and their files unless keep_data
        for d in Data.objects.all():
            if self._keep_data:
                print("KEEPING DATA: {}".format(d.pk))
            elif d.location:
                data_dir = d.location.get_path()
                shutil.rmtree(data_dir, ignore_errors=True)
                d.delete()

        # remove uploaded files
        if not self._keep_data:
            for fn in self._upload_files:
                shutil.rmtree(fn, ignore_errors=True)

        super().tearDown()

        # Check test outcome to prevent failing the test twice.
        # Adapted from: https://stackoverflow.com/a/39606065
        def list2reason(exc_list):
            """Error reason conversion helper."""
            if exc_list and exc_list[-1][0] is self:
                return exc_list[-1][1]

        result = self.defaultTestResult()

        # Python 3.11 has no feedErrorsToResult method.
        if sys.version_info.major == 3 and sys.version_info.minor <= 10:
            self._feedErrorsToResult(result, self._outcome.errors)
        error = list2reason(result.errors)
        failure = list2reason(result.failures)

        # Ensure all tagged processes were tested.
        if (
            not error
            and not failure
            and getattr(settings, "TEST_PROCESS_REQUIRE_TAGS", False)
        ):
            test = getattr(self, self._testMethodName)
            for slug in get_processes_from_tags(test):
                if slug not in self._executed_processes:
                    self.fail(
                        'Test was tagged with process "{}", but this process was not '
                        "executed during test. Remove the tag or test the process.".format(
                            slug
                        )
                    )

    @contextlib.contextmanager
    def preparation_stage(self):
        """Context manager to mark input preparation stage."""
        with self._profiler.add_block("preparation"):
            self._preparation_stage += 1
            try:
                yield
            finally:
                self._preparation_stage -= 1

        # TODO: Handle automatic caching.

    @property
    def files_path(self):
        """Path to test files."""
        if self._files_path is None:
            raise NotImplementedError

        return self._files_path

    @files_path.setter
    def files_path(self, value):
        self._files_path = value

    def run_processor(self, *args, **kwargs):
        """Run process.

        Deprecated method: use run_process.

        """
        return self.run_process(*args, **kwargs)
        # TODO: warning

    def run_process(
        self,
        process_slug,
        input_={},
        assert_status=Data.STATUS_DONE,
        descriptor=None,
        descriptor_schema=None,
        verbosity=0,
        tags=None,
        contributor=None,
        collection=None,
        process_resources=None,
    ):
        """Run the specified process with the given inputs.

        If input is a file, file path should be given relative to the
        ``tests/files`` directory of a Django application.
        If ``assert_status`` is given, check if
        :class:`~resolwe.flow.models.Data` object's status matches
        it after the process has finished.

        .. note::

            If you need to delay calling the manager, you must put the
            desired code in a ``with transaction.atomic()`` block.

        :param str process_slug: slug of the
            :class:`~resolwe.flow.models.Process` to run

        :param dict ``input_``: :class:`~resolwe.flow.models.Process`'s
            input parameters

            .. note::

                You don't have to specify parameters with defined
                default values.

        :param str ``assert_status``: desired status of the
            :class:`~resolwe.flow.models.Data` object

        :param dict descriptor: descriptor to set on the
            :class:`~resolwe.flow.models.Data` object

        :param dict descriptor_schema: descriptor schema to set on the
            :class:`~resolwe.flow.models.Data` object

        :param list tags: list of tags that will be added to the created
            :class:`~resolwe.flow.models.Data` object

        :return: object created by
            :class:`~resolwe.flow.models.Process`
        :rtype: ~resolwe.flow.models.Data

        """
        # Copy input_, to avoid mutation that would occur in ``mock_upload``
        input_ = input_.copy()

        # backward compatibility
        process_slug = slugify(process_slug.replace(":", "-"))

        # Enforce correct process tags.
        if (
            getattr(settings, "TEST_PROCESS_REQUIRE_TAGS", False)
            and not self._preparation_stage
        ):
            test = getattr(self, self._testMethodName)
            if not has_process_tag(test, process_slug):
                self.fail(
                    'Tried to run process with slug "{0}" outside of preparation_stage\n'
                    "block while test is not tagged for this process. Either tag the\n"
                    "test using tag_process decorator or move this under the preparation\n"
                    "stage block if this process is only used to prepare upstream inputs.\n"
                    "\n"
                    "To tag the test you can add the following decorator:\n"
                    "    @tag_process('{0}')\n"
                    "".format(process_slug)
                )

        self._executed_processes.add(process_slug)

        try:
            process = Process.objects.get(slug=process_slug)
        except Process.DoesNotExist:
            self.fail('No process with slug "{}"'.format(process_slug))

        def mock_upload(file_path):
            """Mock file upload."""

            def is_url(path):
                """Check if path is a URL."""
                validate = URLValidator()
                try:
                    validate(path)
                except (ValueError, ValidationError):
                    return False
                return True

            if is_url(file_path):
                return {
                    "file": file_path,
                    "file_temp": file_path,
                    "is_remote": True,
                }
            else:
                old_path = os.path.join(self.files_path, file_path)
                if not os.path.isfile(old_path):
                    raise RuntimeError("Missing file: {}".format(old_path))

                file_basename = os.path.basename(file_path)

                file_temp = "{}_{}".format(file_basename, uuid.uuid4())
                upload_file_path = os.path.join(self.upload_dir, file_temp)
                # create directories needed by new_path
                upload_file_dir = os.path.dirname(upload_file_path)
                if not os.path.exists(upload_file_dir):
                    os.makedirs(upload_file_dir)

                shutil.copy2(old_path, upload_file_path)
                self._upload_files.append(upload_file_path)
                return {
                    "file": file_basename,
                    "file_temp": file_temp,
                }

        for field_schema, fields in iterate_fields(input_, process.input_schema):
            # copy referenced files to upload dir
            if field_schema["type"] == "basic:file:":
                fields[field_schema["name"]] = mock_upload(fields[field_schema["name"]])
            elif field_schema["type"] == "list:basic:file:":
                file_list = [
                    mock_upload(file_path) for file_path in fields[field_schema["name"]]
                ]
                fields[field_schema["name"]] = file_list

            # convert primary keys to strings
            if field_schema["type"].startswith("data:"):
                fields[field_schema["name"]] = fields[field_schema["name"]]
            if field_schema["type"].startswith("list:data:"):
                fields[field_schema["name"]] = [
                    obj for obj in fields[field_schema["name"]]
                ]

        data = Data.objects.create(
            input=input_,
            contributor=contributor or self.admin,
            process=process,
            tags=tags or [],
            descriptor_schema=descriptor_schema,
            descriptor=descriptor or {},
            collection=collection,
            process_resources=process_resources or {},
        )

        # Fetch latest Data object from database
        data = Data.objects.get(pk=data.pk)

        if assert_status:
            if not transaction.get_autocommit() and assert_status == Data.STATUS_DONE:
                # We are in an atomic transaction block, hence the data object will not be done
                # until after the block. Therefore the expected status is resolving.
                assert_status = Data.STATUS_RESOLVING
            self.assertStatus(data, assert_status)

        return data

    def get_json(self, file_name, storage):
        """Return JSON saved in file and test JSON to compare it to.

        The method returns a tuple of the saved JSON and the test JSON.
        In your test you should then compare the test JSON to the saved
        JSON that is commited to the repository.

        The storage argument could be a Storage object, Storage ID or a
        Python dictionary. The test JSON is assigned a json field of
        the Storage object or the complete Python dictionary
        (if a dict is given).

        If the file does not exist it is created, the test JSON is
        written to the new file and an exception is rased.

        :param str file_name: file name (and relative path) of a JSON
            file. Path should be relative to the ``tests/files``
            directory of a Django app. The file name must have a ``.gz`` extension.
        :param storage: Storage object, Storage ID or a dict.
        :type storage: :class:`~resolwe.flow.models.Storage`,
            :class:`str` or :class:`dict`
        :return: (reference JSON, test JSON)
        :rtype: tuple

        """
        self.assertEqual(
            os.path.splitext(file_name)[1], ".gz", msg="File extension must be .gz"
        )

        if isinstance(storage, Storage):
            json_dict = storage.json
        elif isinstance(storage, int):
            json_dict = Storage.objects.get(pk=storage).json
        elif isinstance(storage, dict):
            json_dict = storage
        else:
            raise ValueError("Argument storage should be of type Storage, int or dict.")

        file_path = os.path.join(self.files_path, file_name)
        if not os.path.isfile(file_path):
            with gzip.open(file_path, mode="wt") as f:
                json.dump(json_dict, f)

            self.fail(msg="Output file {} missing so it was created.".format(file_name))

        with gzip.open(file_path, mode="rt") as f:
            return json.load(f), json_dict

    def assertStatus(self, obj, status):
        """Check if object's status is equal to the given status.

        :param obj: object for which to check the status
        :type obj: ~resolwe.flow.models.Data
        :param str status: desired value of object's
            :attr:`~resolwe.flow.models.Data.status` attribute

        """
        self.assertEqual(
            obj.status,
            status,
            msg="Data status is '{}', not '{}'".format(obj.status, status)
            + self._debug_info(obj),
        )

    def _get_output_field(self, obj, path):
        """Return object's output field schema and field dict.

        :param obj: object with the output field
        :type obj: ~resolwe.flow.models.Data

        :param str path: path to :class:`~resolwe.flow.models.Data`
            object's output field

        """
        for field_schema, field, field_path in iterate_fields(
            obj.output, obj.process.output_schema, ""
        ):
            if path == field_path:
                return field_schema, field

        self.fail("Field not found in path {}.".format(path))

    def assertFields(self, obj, path, value):
        """Compare object's field to the given value.

        The file size is ignored. Use assertFile to validate
        file contents.

        :param obj: object with the field to compare
        :type obj: ~resolwe.flow.models.Data

        :param str path: path to
            :class:`~resolwe.flow.models.Data` object's field

        :param str value: desired value of
            :class:`~resolwe.flow.models.Data` object's field

        """
        field_schema, field = None, None
        for field_schema, field, field_path in iterate_schema(
            obj.output, obj.process.output_schema, ""
        ):
            if path == field_path:
                break
        else:
            self.fail("Field not found in path {}".format(path))

        field_name = field_schema["name"]
        field_value = field[field_name]

        def remove_file_size(field_value):
            """Remove size value from file field."""
            if "size" in field_value:
                del field_value["size"]

        # Ignore size in file and dir fields
        if field_schema["type"].startswith("basic:file:") or field_schema[
            "type"
        ].startswith("basic:dir:"):
            remove_file_size(field_value)
            remove_file_size(value)

        elif field_schema["type"].startswith("list:basic:file:") or field_schema[
            "type"
        ].startswith("list:basic:dir:"):
            for val in field_value:
                remove_file_size(val)
            for val in value:
                remove_file_size(val)

        self.assertEqual(
            field_value,
            value,
            msg="Field 'output.{}' mismatch: {} != {}".format(path, field_value, value)
            + self._debug_info(obj),
        )

    def _assert_file(
        self,
        obj,
        fn_tested,
        fn_correct,
        compression=None,
        file_filter=lambda _: False,
        sort=False,
    ):
        """Compare files."""
        open_kwargs = {}
        if compression is None:
            open_fn = open
            # by default, open() will open files as text and return str
            # objects, but we need bytes objects
            open_kwargs["mode"] = "rb"
        elif compression == "gzip":
            open_fn = gzip.open
        elif compression == "zip":
            open_fn = zipfile.ZipFile.open
        else:
            raise ValueError("Unsupported compression format.")

        def get_sha256(filename, **kwargs):
            """Get sha256 for a given file."""
            with open_fn(filename, **kwargs) as handle:
                contents = [line for line in filterfalse(file_filter, handle)]
                if sort:
                    contents = sorted(contents)
                contents = b"".join(contents)
            return hashlib.sha256(contents).hexdigest()

        output = obj.location.get_path(filename=fn_tested)
        output_hash = get_sha256(output, **open_kwargs)

        correct_path = os.path.join(self.files_path, fn_correct)

        if not os.path.isfile(correct_path):
            shutil.copyfile(output, correct_path)
            self.fail(
                msg="Output file {} missing so it was created.".format(fn_correct)
            )

        correct_hash = get_sha256(correct_path, **open_kwargs)
        self.assertEqual(
            correct_hash,
            output_hash,
            msg=(
                f"File contents hash mismatch: {correct_hash} != {output_hash}"
                f" comparing '{output}' with '{correct_path}'."
            )
            + self._debug_info(obj),
        )

    def assertFile(self, obj, field_path, fn, **kwargs):
        """Compare a process's output file to the given correct file.

        :param obj: object that includes the file to compare
        :type obj: ~resolwe.flow.models.Data

        :param str field_path: path to
            :class:`~resolwe.flow.models.Data` object's field with the
            file name

        :param str fn: file name (and relative path) of the correct
            file to compare against. Path should be relative to the
            ``tests/files`` directory of a Django application.

        :param str compression: if not ``None``, files will be
            uncompressed with the appropriate compression library
            before comparison.
            Currently supported compression formats are *gzip* and
            *zip*.

        :param filter: function for filtering the contents of output
            files. It is used in :func:`itertools.filterfalse` function
            and takes one parameter, a line of the output file. If it
            returns ``True``, the line is excluded from comparison of
            the two files.
        :type filter: ~types.FunctionType

        :param bool sort: if set to ``True``, basic sort will be performed
            on file contents before computing hash value.

        """
        field = dict_dot(obj.output, field_path)
        self._assert_file(obj, field["file"], fn, **kwargs)

    def assertFiles(self, obj, field_path, fn_list, **kwargs):
        """Compare a process's output file to the given correct file.

        :param obj: object which includes the files to compare
        :type obj: ~resolwe.flow.models.Data

        :param str field_path: path to
            :class:`~resolwe.flow.models.Data` object's field with the
            list of file names

        :param list fn_list: list of file names (and relative paths) of
            files to compare against. Paths should be relative to the
            ``tests/files`` directory of a Django application.

        :param str compression: if not ``None``, files will be
            uncompressed with the appropriate compression library
            before comparison.
            Currently supported compression formats are *gzip* and
            *zip*.

        :param filter: Function for filtering the contents of output
            files. It is used in :obj:`itertools.filterfalse` function
            and takes one parameter, a line of the output file. If it
            returns ``True``, the line is excluded from comparison of
            the two files.
        :type filter: ~types.FunctionType

        :param bool sort: if set to ``True``, basic sort will be performed
            on file contents before computing hash value.

        """
        field = dict_dot(obj.output, field_path)

        if len(field) != len(fn_list):
            self.fail(
                msg="Lengths of list:basic:file field and files list are not equal."
            )

        for fn_tested, fn_correct in zip(field, fn_list):
            self._assert_file(obj, fn_tested["file"], fn_correct, **kwargs)

    def assertFileExists(self, obj, field_path):
        """Ensure a file in the given object's field exists.

        :param obj: object that includes the file for which to check if
            it exists
        :type obj: ~resolwe.flow.models.Data

        :param str field_path: path to
            :class:`~resolwe.flow.models.Data` object's field with the
            file name/path
        """
        field = dict_dot(obj.output, field_path)
        output = obj.location.get_path(filename=field["file"])

        if not os.path.isfile(output):
            self.fail(msg="File {} does not exist.".format(field_path))

    def assertFilesExist(self, obj, field_path):
        """Ensure files in the given object's field exists.

        :param obj: object that includes list of files for which to check
            existance
        :type obj: ~resolwe.flow.models.Data

        :param str field_path: path to
            :class:`~resolwe.flow.models.Data` object's field with the
            file name/path
        """
        field = dict_dot(obj.output, field_path)

        for item in field:
            output_file = obj.location.get_path(filename=item["file"])
            if not os.path.isfile(output_file):
                self.fail(
                    msg="File {} in output field {} does not exist.".format(
                        item["file"], field_path
                    )
                )

    def assertDirExists(self, obj, field_path):
        """Assert that a directory in the output field of the given object exists.

        :param obj: object that includes the file for which to check if
            it exists

        :param field_path: directory name/path
        """
        schema, field = self._get_output_field(obj, field_path)
        if not schema["type"].startswith("basic:dir:"):
            self.fail(msg="Field {} is not of type basic:dir:".format(field_path))

        dir_path = obj.location.get_path(filename=field[field_path]["dir"])
        if not os.path.isdir(dir_path):
            self.fail(
                msg="Directory {} in output field {} does not exist.".format(
                    dir_path, field_path
                )
            )

    def _assert_dir_structure(self, dir_path, dir_struct, exact=True):
        """Compare tree structure of directory `dir_path` to `dir_struct`."""
        test_dirs = set()
        test_files = set()
        for root, dirs, files in os.walk(dir_path):
            for test_dir in dirs:
                test_dirs.add(os.path.relpath(os.path.join(root, test_dir), dir_path))
            for test_file in files:
                test_files.add(os.path.relpath(os.path.join(root, test_file), dir_path))

        def get_dirs(indict, root=""):
            """Generate directory names from dict."""
            for key, value in indict.items():
                if isinstance(value, dict):
                    yield os.path.join(root, key)
                    yield from get_dirs(value, os.path.join(root, key))
                elif value is not None:
                    self.fail(msg="Directory structure specification is incorrect")

        def get_files(indict, root=""):
            """Generate file names from dict."""
            for key, value in indict.items():
                if value is None:
                    yield os.path.join(root, key)
                elif isinstance(value, dict) and value:
                    yield from get_files(value, os.path.join(root, key))
                elif not isinstance(value, dict):
                    self.fail(msg="Directory structure specification is incorrect.")

        correct_files = {file for file in get_files(dir_struct)}
        correct_dirs = {correct_dir for correct_dir in get_dirs(dir_struct)}

        if exact and (test_dirs != correct_dirs or test_files != correct_files):
            self.fail(msg="Directory structure mismatch (exact check).")
        if not exact and (correct_dirs - test_dirs or correct_files - test_files):
            self.fail(msg="Directory structure mismatch (partial structure check).")

    def assertDirStructure(self, obj, field_path, dir_struct, exact=True):
        """Assert correct tree structure in output field of given object.

        Only names of directories and files are asserted. Content of files is
        not compared.

        :param obj: object that includes the directory to compare
        :type obj: ~resolwe.flow.models.Data

        :param str dir_path: path to the directory to compare

        :param dict dir_struct: correct tree structure of the directory.
            Dictionary keys are directory and file names with the correct nested
            structure. Dictionary value associated with each directory is a new
            dictionary which lists the content of the directory. Dictionary
            value associated with each file name is ``None``

        :param bool exact: if ``True`` tested directory structure must exactly
            match `dir_struct`. If ``False`` `dir_struct` must be a partial
            structure of the directory to compare

        """
        self.assertDirExists(obj, field_path)
        field = dict_dot(obj.output, field_path)
        dir_path = obj.location.get_path(filename=field["dir"])
        self._assert_dir_structure(dir_path, dir_struct, exact)

    def _assert_dir(self, dir_path, fn_correct, fail_on_funny=True):
        """Compare directory `dir_path` to compressed directory `fn_correct`."""
        correct_path = os.path.join(self.files_path, fn_correct)
        if not os.path.isfile(correct_path):
            with tarfile.open(correct_path, "w:gz") as f:
                for content in os.listdir(dir_path):
                    f.add(os.path.join(dir_path, content), arcname=content)
            self.fail(
                msg="Compressed output directory {} missing so it was created.".format(
                    fn_correct
                )
            )

        if not tarfile.is_tarfile(correct_path):
            self.fail(msg="{} is not a tar file.".format(fn_correct))

        with tempfile.TemporaryDirectory() as temp_dir:
            with tarfile.open(correct_path) as tar:
                tar.extractall(temp_dir)
            cmp = filecmp.dircmp(dir_path, temp_dir)
            if (
                cmp.left_only
                or cmp.right_only
                or cmp.diff_files
                or (fail_on_funny and cmp.funny_files)
            ):
                self.fail(
                    msg="Directory {} content mismatch: {}.".format(
                        fn_correct, cmp.report()
                    )
                )

    def assertDir(self, obj, field_path, fn):
        """Compare process output directory to correct compressed directory.

        :param obj: object that includes the directory to compare
        :type obj: ~resolwe.flow.models.Data

        :param str field_path: path to
            :class:`~resolwe.flow.models.Data` object's field with the
            file name

        :param str fn: file name (and relative path) of the correct compressed
            directory to compare against. Path should be relative to the
            ``tests/files`` directory of a Django application. Compressed
            directory needs to be in ``tar.gz`` format.

        """
        self.assertDirExists(obj, field_path)
        field = dict_dot(obj.output, field_path)
        dir_path = obj.location.get_path(filename=field["dir"])
        self._assert_dir(dir_path, fn)

    def assertJSON(self, obj, storage, field_path, file_name):
        """Compare JSON in Storage object to the given correct JSON.

        :param obj: object to which the
            :class:`~resolwe.flow.models.Storage` object belongs
        :type obj: ~resolwe.flow.models.Data

        :param storage: object or id which contains JSON to compare
        :type storage: :class:`~resolwe.flow.models.Storage` or
            :class:`str`

        :param str field_path: path to JSON subset in the
            :class:`~resolwe.flow.models.Storage`'s object to compare
            against. If it is empty, the entire object will be
            compared.

        :param str file_name: file name (and relative path) of the file
            with the correct JSON to compare against. Path should be
            relative to the ``tests/files`` directory of a Django
            application.

            .. note::

                The given JSON file should be compresed with *gzip* and
                have the ``.gz`` extension.

        """
        self.assertEqual(
            os.path.splitext(file_name)[1], ".gz", msg="File extension must be .gz"
        )

        if not isinstance(storage, Storage):
            storage = Storage.objects.get(pk=storage)

        storage_obj = dict_dot(storage.json, field_path)

        file_path = os.path.join(self.files_path, file_name)
        if not os.path.isfile(file_path):
            with gzip.open(file_path, mode="wt") as f:
                json.dump(storage_obj, f)

            self.fail(msg="Output file {} missing so it was created.".format(file_name))

        with gzip.open(file_path, mode="rt") as f:
            file_obj = json.load(f)

        self.assertAlmostEqualGeneric(
            storage_obj,
            file_obj,
            msg="Storage {} field '{}' does not match file {}".format(
                storage.id, field_path, file_name
            )
            + self._debug_info(obj),
        )

    def _debug_info(self, data):
        """Return data's debugging information."""
        msg_header = "Debugging information for data object {}".format(data.pk)
        msg = (
            "\n\n"
            + len(msg_header) * "="
            + "\n"
            + msg_header
            + "\n"
            + len(msg_header) * "="
            + "\n"
        )
        path = data.location.get_path(filename="stdout.txt") if data.location else None
        if path and os.path.isfile(path):
            msg += "\nstdout.txt:\n" + 11 * "-" + "\n"
            with io.open(path, mode="rt") as fn:
                msg += fn.read()

        if data.process_error:
            msg += "\nProcess' errors:\n" + 16 * "-" + "\n"
            msg += "\n".join(data.process_error)

        if data.process_warning:
            msg += "\nProcess' warnings:\n" + 18 * "-" + "\n"
            msg += "\n".join(data.process_warning)

        return msg
