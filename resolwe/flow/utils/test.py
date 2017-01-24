""".. Ignore pydocstyle D400.

=======
Testing
=======

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import copy
import hashlib
import gzip
import io
import json
import os
import shlex
import shutil
import subprocess
import zipfile
import unittest

import six
from six.moves import filterfalse

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core import management
from django.test import TestCase, override_settings
from django.utils.crypto import get_random_string
from django.utils.text import slugify

from resolwe.flow.models import (Data, dict_dot, iterate_fields, iterate_schema, Collection,
                                 DescriptorSchema, Process, Storage)
from resolwe.flow.managers import manager

if six.PY2:
    # Monkey-patch shutil package with which function (available in Python 3.3+)
    import shutilwhich  # pylint: disable=import-error,unused-import


SCHEMAS_FIXTURE_CACHE = None


def check_installed(command):
    """Check if the given command is installed.

    :param str command: name of the command

    :return: (indicator of the availability of the command, message saying
              command is not available)
    :rtype: tuple(bool, str)

    """
    if shutil.which(command):
        return True, ""
    else:
        return False, "Command '{}' is not found.".format(command)


def check_docker():
    """Check if Docker is installed and working.

    :return: (indicator of the availability of Docker, reason for
              unavailability)
    :rtype: tuple(bool, str)

    """
    command = getattr(settings, 'FLOW_EXECUTOR', {}).get('COMMAND', 'docker')
    info_command = '{} info'.format(command)
    available, reason = True, ""
    # TODO: Use subprocess.DEVNULL after dropping support for Python 2
    with open(os.devnull, 'wb') as DEVNULL:  # pylint: disable=invalid-name
        try:
            subprocess.check_call(shlex.split(info_command), stdout=DEVNULL, stderr=subprocess.STDOUT)
        except OSError:
            available, reason = False, "Docker command '{}' not found".format(command)
        except subprocess.CalledProcessError:
            available, reason = (False, "Docker command '{}' returned non-zero "
                                        "exit status".format(info_command))
    return available, reason


def with_docker_executor(method):
    """Decorate unit test to run processes with Docker executor."""
    # pylint: disable=missing-docstring
    @unittest.skipUnless(*check_docker())
    def wrapper(*args, **kwargs):
        executor_settings = settings.FLOW_EXECUTOR.copy()
        executor_settings.update({
            'NAME': 'resolwe.flow.executors.docker',
            'CONTAINER_IMAGE': 'resolwe/test:base'
        })

        try:
            with override_settings(FLOW_EXECUTOR=executor_settings):
                # Re-run engine discovery as the settings have changed.
                manager.discover_engines()

                # Run the actual unit test method.
                method(*args, **kwargs)
        finally:
            # Re-run engine discovery as the settings have changed.
            manager.discover_engines()

    return wrapper


# override all FLOW_EXECUTOR settings that are specified in FLOW_EXECUTOR['TEST']
FLOW_EXECUTOR_SETTINGS = copy.copy(getattr(settings, 'FLOW_EXECUTOR', {}))
TEST_SETTINGS_OVERRIDES = getattr(settings, 'FLOW_EXECUTOR', {}).get('TEST', {})
FLOW_EXECUTOR_SETTINGS.update(TEST_SETTINGS_OVERRIDES)

# update FLOW_DOCKER_MAPPINGS setting if necessary
FLOW_DOCKER_MAPPINGS = copy.copy(getattr(settings, 'FLOW_DOCKER_MAPPINGS', {}))
for map_ in FLOW_DOCKER_MAPPINGS:
    for map_entry in ['src', 'dest']:
        for setting in ['DATA_DIR', 'UPLOAD_DIR']:
            if settings.FLOW_EXECUTOR[setting] in map_[map_entry]:
                map_[map_entry] = map_[map_entry].replace(
                    settings.FLOW_EXECUTOR[setting], FLOW_EXECUTOR_SETTINGS[setting], 1)


@override_settings(FLOW_EXECUTOR=FLOW_EXECUTOR_SETTINGS)
@override_settings(FLOW_DOCKER_MAPPINGS=FLOW_DOCKER_MAPPINGS)
@override_settings(CELERY_ALWAYS_EAGER=True)
class ProcessTestCase(TestCase):
    """Base class for writing process tests.

    This class is subclass of Django's :class:`~django.test.TestCase`
    with some specific functions used for testing processes.

    To write a process test use standard Django's syntax for writing
    tests and follow the next steps:

    #. Put input files (if any) in ``tests/files`` directory of a
       Django application.
    #. Run the process using :meth:`run_process`.
    #. Check if the process has the expected status using
       :meth:`assertStatus`.
    #. Check process's output using :meth:`assertFields`,
       :meth:`assertFile`, :meth:`assertFileExists`,
       :meth:`assertFiles` and :meth:`assertJSON`.

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

    def _register_schemas(self, **kwargs):
        """Register process and descriptor schemas.

        Process and DescriptorSchema cached to SCHEMAS_FIXTURE_CACHE
        global variable based on ``path`` key in ``kwargs``.

        """
        Process.objects.all().delete()

        cache_key = json.dumps(kwargs)
        global SCHEMAS_FIXTURE_CACHE  # pylint: disable=global-statement
        if not SCHEMAS_FIXTURE_CACHE:
            SCHEMAS_FIXTURE_CACHE = {}

        if cache_key in SCHEMAS_FIXTURE_CACHE:
            process_schemas = SCHEMAS_FIXTURE_CACHE[cache_key]['processes']
            self._update_schema_relations(process_schemas)
            Process.objects.bulk_create(process_schemas)

            descriptor_schemas = SCHEMAS_FIXTURE_CACHE[cache_key]['descriptor_schemas']
            self._update_schema_relations(descriptor_schemas)
            DescriptorSchema.objects.bulk_create(descriptor_schemas)
        else:
            management.call_command('register', force=True, testing=True, verbosity='0', **kwargs)

            if cache_key not in SCHEMAS_FIXTURE_CACHE:
                SCHEMAS_FIXTURE_CACHE[cache_key] = {}

            # list forces db query execution
            SCHEMAS_FIXTURE_CACHE[cache_key]['processes'] = list(Process.objects.all())
            SCHEMAS_FIXTURE_CACHE[cache_key]['descriptor_schemas'] = list(DescriptorSchema.objects.all())

    def setUp(self):
        """Initialize test data."""
        super(ProcessTestCase, self).setUp()

        self.admin = get_user_model().objects.create_superuser(
            username="admin", email='admin@example.com', password="admin_pass")
        self._register_schemas()

        self.collection = Collection.objects.create(contributor=self.admin, name="Test collection")
        self.files_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files')
        self.upload_dir = settings.FLOW_EXECUTOR['UPLOAD_DIR']
        self._keep_all = False
        self._keep_failed = False
        self._upload_files = []

        # create upload dir if it doesn't exist
        if not os.path.isdir(self.upload_dir):
            os.mkdir(self.upload_dir)

    def tearDown(self):
        """Clean up after the test."""
        super(ProcessTestCase, self).tearDown()

        # Delete Data objects and their files unless keep_failed
        for d in Data.objects.all():
            if self._keep_all or (self._keep_failed and d.status == "error"):
                print("KEEPING DATA: {}".format(d.pk))
            else:
                data_dir = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(d.pk))
                export_dir = os.path.join(settings.FLOW_EXECUTOR['UPLOAD_DIR'], str(d.pk))
                d.delete()
                shutil.rmtree(data_dir, ignore_errors=True)
                shutil.rmtree(export_dir, ignore_errors=True)

        # remove uploaded files
        if not self._keep_all and not self._keep_failed:
            for fn in self._upload_files:
                shutil.rmtree(fn, ignore_errors=True)

    def keep_all(self):
        """Do not delete output files after test for all data."""
        self._keep_all = True

    def keep_failed(self):
        """Do not delete output files after test for failed data."""
        self._keep_failed = True

    def run_processor(self, *args, **kwargs):
        """Deprecated method: use run_process."""
        return self.run_process(*args, **kwargs)
        # TODO: warning

    def run_process(self, process_slug, input_={}, assert_status=Data.STATUS_DONE,
                    descriptor=None, descriptor_schema=None, run_manager=True,
                    verbosity=0):
        """Run the specified process with the given inputs.

        If input is a file, file path should be given relative to the
        ``tests/files`` directory of a Django application.
        If ``assert_status`` is given, check if
        :class:`~resolwe.flow.models.Data` object's status matches
        it after the process has finished.

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

        :return: object created by
            :class:`~resolwe.flow.models.Process`
        :rtype: ~resolwe.flow.models.Data

        """
        # backward compatibility
        process_slug = slugify(process_slug.replace(':', '-'))

        process = Process.objects.filter(slug=process_slug).order_by('-version').first()

        def mock_upload(file_path):
            """Mock file upload."""
            old_path = os.path.join(self.files_path, file_path)
            if not os.path.isfile(old_path):
                raise RuntimeError('Missing file: {}'.format(old_path))

            new_path = os.path.join(self.upload_dir, file_path)
            # create directories needed by new_path
            new_path_dir = os.path.dirname(new_path)
            if not os.path.exists(new_path_dir):
                os.makedirs(new_path_dir)
            shutil.copy2(old_path, new_path)
            self._upload_files.append(new_path)
            return {
                'file': file_path,
                'file_temp': file_path,
            }

        for field_schema, fields in iterate_fields(input_, process.input_schema):
            # copy referenced files to upload dir
            if field_schema['type'] == "basic:file:":
                fields[field_schema['name']] = mock_upload(fields[field_schema['name']])
            elif field_schema['type'] == "list:basic:file:":
                file_list = [mock_upload(file_path) for file_path in fields[field_schema['name']]]
                fields[field_schema['name']] = file_list

            # convert primary keys to strings
            if field_schema['type'].startswith('data:'):
                fields[field_schema['name']] = fields[field_schema['name']]
            if field_schema['type'].startswith('list:data:'):
                fields[field_schema['name']] = [obj for obj in fields[field_schema['name']]]

        data = Data.objects.create(
            input=input_,
            contributor=self.admin,
            process=process,
            slug=get_random_string(length=6),
            descriptor_schema=descriptor_schema,
            descriptor=descriptor or {})
        self.collection.data.add(data)

        if run_manager:
            manager.communicate(run_sync=True, verbosity=verbosity)

        # Fetch latest Data object from database
        data = Data.objects.get(pk=data.pk)
        if not run_manager and assert_status == Data.STATUS_DONE:
            assert_status = Data.STATUS_RESOLVING

        if assert_status:
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
        self.assertEqual(os.path.splitext(file_name)[1], '.gz', msg='File extension must be .gz')

        if isinstance(storage, Storage):
            json_dict = storage.json
        elif isinstance(storage, int):
            json_dict = Storage.objects.get(pk=storage).json
        elif isinstance(storage, dict):
            json_dict = storage
        else:
            raise ValueError('Argument storage should be of type Storage, int or dict.')

        file_path = os.path.join(self.files_path, file_name)
        if not os.path.isfile(file_path):
            with gzip.open(file_path, mode='wt') as f:
                json.dump(json_dict, f)

            self.fail(msg="Output file {} missing so it was created.".format(file_name))

        with gzip.open(file_path, mode='rt') as f:
            return json.load(f), json_dict

    def assertStatus(self, obj, status):  # pylint: disable=invalid-name
        """Check if object's status is equal to the given status.

        :param obj: object for which to check the status
        :type obj: ~resolwe.flow.models.Data
        :param str status: desired value of object's
            :attr:`~resolwe.flow.models.Data.status` attribute

        """
        self.assertEqual(obj.status, status,
                         msg="Data status is '{}', not '{}'".format(obj.status, status) +
                         self._debug_info(obj))

    def assertFields(self, obj, path, value):  # pylint: disable=invalid-name
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
        for field_schema, field, field_path in iterate_schema(obj.output, obj.process.output_schema, ''):
            if path == field_path:
                break
        else:
            self.fail("Field not found in path {}".format(path))

        field_name = field_schema['name']
        field_value = field[field_name]

        def remove_file_size(field_value):
            """Remove size value from file field."""
            if 'size' in field_value:
                del field_value['size']

        # Ignore size in file and dir fields
        if (field_schema['type'].startswith('basic:file:') or
                field_schema['type'].startswith('basic:dir:')):
            remove_file_size(field_value)
            remove_file_size(value)

        elif (field_schema['type'].startswith('list:basic:file:') or
              field_schema['type'].startswith('list:basic:dir:')):
            for val in field_value:
                remove_file_size(val)
            for val in value:
                remove_file_size(val)

        self.assertEqual(field_value, value,
                         msg="Field 'output.{}' mismatch: {} != {}".format(path, field_value, value) +
                         self._debug_info(obj))

    def _assert_file(self, obj, fn_tested, fn_correct, compression=None, file_filter=lambda _: False):
        """Compare files."""
        open_kwargs = {}
        if compression is None:
            open_fn = open
            # by default, open() will open files as text and return str
            # objects, but we need bytes objects
            open_kwargs['mode'] = 'rb'
        elif compression == 'gzip':
            open_fn = gzip.open
        elif compression == 'zip':
            open_fn = zipfile.ZipFile.open
        else:
            raise ValueError("Unsupported compression format.")

        output = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(obj.pk), fn_tested)
        with open_fn(output, **open_kwargs) as output_file:
            output_contents = b"".join([line for line in filterfalse(file_filter, output_file)])
        output_hash = hashlib.sha256(output_contents).hexdigest()

        correct_path = os.path.join(self.files_path, fn_correct)

        if not os.path.isfile(correct_path):
            shutil.copyfile(output, correct_path)
            self.fail(msg="Output file {} missing so it was created.".format(fn_correct))

        with open_fn(correct_path, **open_kwargs) as correct_file:
            correct_contents = b"".join([line for line in filterfalse(file_filter, correct_file)])
        correct_hash = hashlib.sha256(correct_contents).hexdigest()
        self.assertEqual(correct_hash, output_hash,
                         msg="File contents hash mismatch: {} != {}".format(
                             correct_hash, output_hash) + self._debug_info(obj))

    def assertFile(self, obj, field_path, fn, **kwargs):  # pylint: disable=invalid-name
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

        """
        field = dict_dot(obj.output, field_path)
        self._assert_file(obj, field['file'], fn, **kwargs)

    def assertFiles(self, obj, field_path, fn_list, **kwargs):  # pylint: disable=invalid-name
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

        """
        field = dict_dot(obj.output, field_path)

        if len(field) != len(fn_list):
            self.fail(msg="Lengths of list:basic:file field and files list are not equal.")

        for fn_tested, fn_correct in zip(field, fn_list):
            self._assert_file(obj, fn_tested['file'], fn_correct, **kwargs)

    def assertFileExists(self, obj, field_path):  # pylint: disable=invalid-name
        """Ensure a file in the given object's field exists.

        :param obj: object that includes the file for which to check if
            it exists
        :type obj: ~resolwe.flow.models.Data

        :param str field_path: path to
            :class:`~resolwe.flow.models.Data` object's field with the
            file name/path
        """
        field = dict_dot(obj.output, field_path)
        output = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(obj.pk), field['file'])

        if not os.path.isfile(output):
            self.fail(msg="File {} does not exist.".format(field_path))

    def assertJSON(self, obj, storage, field_path, file_name):  # pylint: disable=invalid-name
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
        self.assertEqual(os.path.splitext(file_name)[1], '.gz', msg='File extension must be .gz')

        if not isinstance(storage, Storage):
            storage = Storage.objects.get(pk=storage)

        storage_obj = dict_dot(storage.json, field_path)

        file_path = os.path.join(self.files_path, file_name)
        if not os.path.isfile(file_path):
            with gzip.open(file_path, mode='wt') as f:
                json.dump(storage_obj, f)

            self.fail(msg="Output file {} missing so it was created.".format(file_name))

        with gzip.open(file_path, mode='rt') as f:
            file_obj = json.load(f)

        self.assertEqual(storage_obj, file_obj,
                         msg="Storage {} field '{}' does not match file {}".format(
                             storage.id, field_path, file_name) + self._debug_info(obj))

    def _debug_info(self, data):
        """Return data's debugging information."""
        msg_header = "Debugging information for data object {}".format(data.pk)
        msg = "\n\n" + len(msg_header) * "=" + "\n" + msg_header + "\n" + len(msg_header) * "=" + "\n"
        path = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(data.pk), "stdout.txt")
        if os.path.isfile(path):
            msg += "\nstdout.txt:\n" + 11 * "-" + "\n"
            with io.open(path, mode='rt') as fn:
                msg += fn.read()

        if data.process_error:
            msg += "\nProcess' errors:\n" + 16 * "-" + "\n"
            msg += "\n".join(data.process_error)

        if data.process_warning:
            msg += "\nProcess' warnings:\n" + 18 * "-" + "\n"
            msg += "\n".join(data.process_warning)

        return msg
