"""
=======
Testing
=======

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import copy
import hashlib
import gzip
import json
import os
import shutil
from six.moves import filterfalse
import stat
import zipfile

from django.apps import apps
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core import management
from django.test import TestCase, override_settings
from django.utils.crypto import get_random_string
from django.utils.text import slugify

from resolwe.flow.models import Data, dict_dot, iterate_fields, Collection, DescriptorSchema, Process, Storage
from resolwe.flow.managers import manager


SCHEMAS_FIXTURE_CACHE = None


def _register_schemas():
    """Register process and descriptor schemas.

    Process and DescriptorSchema definitions are registered when first
    test is callled and cached to SCHEMAS_FIXTURE_CACHE global variable

    """
    Process.objects.all().delete()

    global SCHEMAS_FIXTURE_CACHE  # pylint: disable=global-statement
    if SCHEMAS_FIXTURE_CACHE:
        Process.objects.bulk_create(SCHEMAS_FIXTURE_CACHE['processes'])
        DescriptorSchema.objects.bulk_create(SCHEMAS_FIXTURE_CACHE['descriptor_schemas'])
    else:
        user_model = get_user_model()

        if not user_model.objects.filter(is_superuser=True).exists():
            user_model.objects.create_superuser(username="admin", email='admin@example.com', password="admin_pass")

        management.call_command('register', force=True, testing=True, verbosity='0')

        SCHEMAS_FIXTURE_CACHE = {}
        SCHEMAS_FIXTURE_CACHE['processes'] = list(Process.objects.all())  # list forces db query execution
        SCHEMAS_FIXTURE_CACHE['descriptor_schemas'] = list(DescriptorSchema.objects.all())


# override all FLOW_EXECUTOR settings that are specified in FLOW_EXECUTOR['TEST']
flow_executor_settings = settings.FLOW_EXECUTOR.copy()
test_settings_overrides = settings.FLOW_EXECUTOR.get('TEST', {})
flow_executor_settings.update(test_settings_overrides)

# update FLOW_DOCKER_MAPPINGS setting if necessary
flow_docker_mappings = copy.copy(getattr(settings, 'FLOW_DOCKER_MAPPINGS', {}))
for map_ in flow_docker_mappings:
    for map_entry in ['src', 'dest']:
        for setting in ['DATA_DIR', 'UPLOAD_DIR']:
            if settings.FLOW_EXECUTOR[setting] in map_[map_entry]:
                map_[map_entry] = map_[map_entry].replace(
                    settings.FLOW_EXECUTOR[setting], flow_executor_settings[setting], 1)


@override_settings(FLOW_EXECUTOR=flow_executor_settings)
@override_settings(FLOW_DOCKER_MAPPINGS=flow_docker_mappings)
@override_settings(CELERY_ALWAYS_EAGER=True)
class ProcessTestCase(TestCase):

    """Base class for writing processor tests.

    This class is subclass of Django's ``TestCase`` with some specific
    functions used for testing processors.

    To write a processor test use standard Django's syntax for writing
    tests and follow next steps:

    #. Put input files (if any) in ``tests/files`` folder of a Django
       application.
    #. Run test with run_processor.
    #. Check if processor has finished successfully with
       assertDone function.
    #. Assert processor's output with :func:`assertFiles`,
       :func:`assertFields` and :func:`assertJSON` functions.

    .. note::
        When creating a test case for a custom Django application,
        subclass this class and over-ride the ``self.files_path`` with:

        .. code-block:: python

            self.files_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files')

    .. DANGER::
        If output files don't exist in ``tests/files`` folder of a
        Django application, they are created automatically.
        But you have to check that they are correct before using them
        for further runs.

    """

    def setUp(self):
        super(ProcessTestCase, self).setUp()
        self.admin = get_user_model().objects.create_superuser(
            username="admin", email='admin@example.com', password="admin_pass")
        _register_schemas()

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
        super(ProcessTestCase, self).tearDown()

        # Delete Data objects and their files unless keep_failed
        for d in Data.objects.all():
            if self._keep_all or (self._keep_failed and d.status == "error"):
                print("KEEPING DATA: {}".format(d.pk))
            else:
                data_dir = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(d.pk))
                d.delete()
                shutil.rmtree(data_dir, ignore_errors=True)

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
        return self.run_process(*args, **kwargs)
        # TODO: warning

    def run_process(self, process_slug, input_={}, descriptor=None, descriptor_schema=None,
                    assert_status=Data.STATUS_DONE, run_manager=True, verbosity=0):
        """Runs given processor with specified inputs.

        If input is file, file path should be given relative to
        ``tests/files`` folder of a Django application.
        If ``assert_status`` is given check if Data object's status
        matches ``assert_status`` after finishing processor.

        :param processor_name: name of the processor to run
        :type processor_name: :obj:`str`

        :param ``input_``: Input paramaters for processor. You don't
            have to specifie parameters for which default values are
            given.
        :type ``input_``: :obj:`dict`

        :param descriptor: Descriptor to set on the data object.
        :type descriptor: :obj:`dict`

        :param descriptor_schema: Descriptor schema to set on the data object.
        :type descriptor_schema: :obj:`dict`

        :param ``assert_status``: Desired status of Data object
        :type ``assert_status``: :obj:`str`

        :return: :obj:`resolwe.flow.models.Data` object which is created by
            the processor.

        """

        # backward compatibility
        process_slug = slugify(process_slug.replace(':', '-'))

        p = Process.objects.get(slug=process_slug)

        def mock_upload(file_path):
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

        for field_schema, fields in iterate_fields(input_, p.input_schema):
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

        d = Data.objects.create(
            input=input_,
            contributor=self.admin,
            process=p,
            slug=get_random_string(length=6),
            descriptor_schema=descriptor_schema,
            descriptor=descriptor or {})
        self.collection.data.add(d)

        if run_manager:
            manager.communicate(run_sync=True, verbosity=verbosity)

        # Fetch latest Data object from database
        d = Data.objects.get(pk=d.pk)
        if not run_manager and assert_status == Data.STATUS_DONE:
            assert_status = Data.STATUS_RESOLVING

        if assert_status:
            self.assertStatus(d, assert_status)

        return d

    def assertStatus(self, obj, status):  # pylint: disable=invalid-name
        """Check if Data object's status is 'status'.

        :param obj: Data object for which to check status
        :type obj: :obj:`resolwe.flow.models.Data`
        :param status: Data status to check
        :type status: str

        """
        self.assertEqual(obj.status, status,
                         msg="Data status is '{}', not '{}'".format(obj.status, status) +
                         self._debug_info(obj))

    def assertFields(self, obj, path, value):  # pylint: disable=invalid-name
        """Compare Data object's field to given value.

        :param obj: Data object with field to compare
        :type obj: :obj:`resolwe.flow.models.Data`

        :param path: Path to field in Data object.
        :type path: :obj:`str`

        :param value: Desired value.
        :type value: :obj:`str`

        """
        field = dict_dot(obj.output, path)
        self.assertEqual(field, value,
                         msg="Field 'output.{}' mismatch: {} != {}".format(path, field, value) +
                         self._debug_info(obj))

    def _assert_file(self, obj, fn_tested, fn_correct, compression=None, filter=lambda _: False):
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
            output_contents = b"".join([line for line in filterfalse(filter, output_file)])
        output_hash = hashlib.sha256(output_contents).hexdigest()

        correct_path = os.path.join(self.files_path, fn_correct)

        if not os.path.isfile(correct_path):
            shutil.copyfile(output, correct_path)
            self.fail(msg="Output file {} missing so it was created.".format(fn_correct))

        with open_fn(correct_path, **open_kwargs) as correct_file:
            correct_contents = b"".join([line for line in filterfalse(filter, correct_file)])
        correct_hash = hashlib.sha256(correct_contents).hexdigest()
        self.assertEqual(correct_hash, output_hash,
                         msg="File contents hash mismatch: {} != {}".format(
                             correct_hash, output_hash) + self._debug_info(obj))

    def assertFile(self, obj, field_path, fn, **kwargs):  # pylint: disable=invalid-name
        """Compare process's output file to the given correct file

        :param obj: Data object which includes file that we want to
            compare.
        :type obj: :obj:`resolwe.flow.models.Data`

        :param str field_path: Path to list of file names in Data object.

        :param str fn: File name (and relative path) of file to which we
            want to compare. Name/path is relative to ``tests/files``
            folder of a Django application.

        :param compression: If not None, files will be uncompressed with
            the appropriate compression library before comparison.
            Currently supported compression formats are "gzip" and
            "zip".
        :type compression: :obj:`str`

        :param filter: Function for filtering the contents of output
            files. It is used in :obj:`itertools.filterfalse` function
            and takes one parameter, a line of the output file. If it
            returns `True`, the line is excluded from comparison of the
            two files.
        :type filter: :obj:`function`

        """
        field = dict_dot(obj.output, field_path)
        self._assert_file(obj, field['file'], fn, **kwargs)

    def assertFiles(self, obj, field_path, fn_list, **kwargs):  # pylint: disable=invalid-name
        """Compare list of processes' output files to the given correct files

        :param obj: Data object which includes files that we want to
            compare.
        :type obj: :obj:`resolwe.flow.models.Data`

        :param str field_path: Path to list of file names in Data object.

        :param list fn_list: List od file names (and relative paths) of
            files to which we want to compare. Name/path is relative to
            ``tests/files`` folder of a Django application.

        :param compression: If not None, files will be uncompressed with
            the appropriate compression library before comparison.
            Currently supported compression formats are "gzip" and
            "zip".
        :type compression: :obj:`str`

        :param filter: Function for filtering the contents of output
            files. It is used in :obj:`itertools.filterfalse` function
            and takes one parameter, a line of the output file. If it
            returns `True`, the line is excluded from comparison of the
            two files.
        :type filter: :obj:`function`

        """
        field = dict_dot(obj.output, field_path)

        if len(field) != len(fn_list):
            self.fail(msg="Lengths of list:basic:file field and files list are not equal.")

        for fn_tested, fn_correct in zip(field, fn_list):
            self._assert_file(obj, fn_tested['file'], fn_correct, **kwargs)

    def assertFileExists(self, obj, field_path):  # pylint: disable=invalid-name
        """Compare output file of a processor to the given correct file.

        :param obj: Data object which includes file that we want to
            compare.
        :type obj: :obj:`resolwe.flow.models.Data`

        :param field_path: Path to file name in Data object.
        :type field_path: :obj:`str`

        """
        field = dict_dot(obj.output, field_path)
        output = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(obj.pk), field['file'])

        if not os.path.isfile(output):
            self.fail(msg="File {} does not exist.".format(field_path))

    def assertJSON(self, obj, storage, field_path, file_name):  # pylint: disable=invalid-name
        """Compare JSON in Storage object to the given correct output.

        :param obj: Data object which includes file that we want to
            compare.
        :type obj: :obj:`resolwe.flow.models.Data`

        :param storage: Storage (or storage id) which contains JSON to
            compare.
        :type storage: :obj:`resolwe.flow.models.Storage` or :obj:`str`

        :param field_path: Path to JSON subset to compare in Storage
            object. If it is empty, entire Storage object will be
            compared.
        :type field_path: :obj:`str`

        :param file_name: File name (and relative path) of file to
            which we want to compare. Name/path is relative to
            ``tests/files`` folder of a Django application.
        :type file_name: :obj:`str`

        """
        self.assertEqual(os.path.splitext(file_name)[1], '.gz', msg='File extension must be .gz')

        if not isinstance(storage, Storage):
            storage = Storage.objects.get(pk=storage)

        storage_obj = dict_dot(storage.json, field_path)

        file_path = os.path.join(self.files_path, file_name)
        if not os.path.isfile(file_path):
            with gzip.open(file_path, 'w') as f:
                json.dump(storage_obj, f)

            self.fail(msg="Output file {} missing so it was created.".format(file_name))

        with gzip.open(file_path) as f:
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
            with open(path, 'r') as fn:
                msg += fn.read()

        if data.process_error:
            msg += "\nProcess' errors:\n" + 16 * "-" + "\n"
            msg += "\n".join(data.process_error)

        if data.process_warning:
            msg += "\nProcess' warnings:\n" + 18 * "-" + "\n"
            msg += "\n".join(data.process_warning)

        return msg
