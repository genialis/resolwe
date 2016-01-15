"""
.. autoclass:: resolwe.flow.tests.BaseProcessorTestCase
   :members:

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import hashlib
import gzip
import json
import os
import shutil
import stat
import zipfile

from django.apps import apps
from django.conf import settings
from django.contrib.auth import get_user_model
from django.core import management
from django.test import TestCase, override_settings
from django.utils.crypto import get_random_string
from django.utils.text import slugify

from resolwe.flow.models import Data, dict_dot, iterate_fields, Process, Project, Storage
from resolwe.flow.engines.local import manager


PROCESSES_FIXTURE_CACHE = None


def _register_processors():
    """Register processors.

    Processor definitions are red when the first test is callled and cached
    into the PROCESSORS_FIXTURE_CACHE global variable.

    """
    Process.objects.all().delete()

    global PROCESSES_FIXTURE_CACHE  # pylint: disable=global-statement
    if PROCESSES_FIXTURE_CACHE:
        Process.objects.bulk_create(PROCESSES_FIXTURE_CACHE)
    else:
        user_model = get_user_model()

        if not user_model.objects.filter(is_superuser=True).exists():
            user_model.objects.create_superuser(username="admin", email='admin@example.com', password="admin_pass")

        management.call_command('process_register', force=True, testing=True, verbosity='0')

        PROCESSES_FIXTURE_CACHE = list(Process.objects.all())  # list forces db query execution


flow_executor_settings = settings.FLOW_EXECUTOR.copy()
# since we don't know what uid/gid will be used inside Docker executor, others
# must have all permissions on the data directory
flow_executor_settings['DATA_DIR_MODE'] = 0o777


@override_settings(FLOW_EXECUTOR=flow_executor_settings)
class ProcessTestCase(TestCase):

    """Base class for writing processor tests.

    This class is subclass of Django's ``TestCase`` with some specific
    functions used for testing processors.

    To write a processor test use standard Django's syntax for writing
    tests and follow next steps:

    #. Put input files (if any) in ``server/tests/processor/inputs``
       folder.
    #. Run test with run_processor.
    #. Check if processor has finished successfully with
       assertDone function.
    #. Assert processor's output with :func:`assertFiles`,
       :func:`assertFields` and :func:`assertJSON` functions.

    .. DANGER::
        If output files doesn't exists in
        ``server/tests/processor/outputs`` folder, they are created
        automatically. But you have to chack that they are correct
        before using them for further runs.

    """

    def setUp(self):
        super(ProcessTestCase, self).setUp()
        self.admin = get_user_model().objects.create_superuser(
            username="admin", email='admin@example.com', password="admin_pass")
        _register_processors()

        self.project = Project.objects.create(contributor=self.admin)
        self.current_path = os.path.dirname(os.path.abspath(__file__))
        self._keep_all = False
        self._keep_failed = False

    def tearDown(self):
        super(ProcessTestCase, self).tearDown()

        # Delete Data objects and their files unless keep_failed
        for d in Data.objects.all():
            if self._keep_all or (self._keep_failed and d.status == "error"):
                print("KEEPING DATA: {}".format(d.pk))
            else:
                data_dir = os.path.join(settings.FLOW_EXECUTOR['DATA_PATH'], str(d.pk))
                d.delete()
                shutil.rmtree(data_dir, ignore_errors=True)

    def keep_all(self):
        """Do not delete output files after test for all data."""
        self._keep_all = True

    def keep_failed(self):
        """Do not delete output files after test for failed data."""
        self._keep_failed = True

    def run_processor(self, *args, **kwargs):
        self.run_process(*args, **kwargs)
        # TODO: warning

    def run_process(self, process_slug, input_={}, assert_status=Data.STATUS_DONE, run_manager=True, verbosity=0):
        """Runs given processor with specified inputs.

        If input is file, file path should be given relative to
        ``tests/files`` folder.
        If ``assert_status`` is given check if Data object's status
        matches ``assert_status`` after finishing processor.

        :param processor_name: name of the processor to run
        :type processor_name: :obj:`str`

        :param ``input_``: Input paramaters for processor. You don't
            have to specifie parameters for which default values are
            given.
        :type ``input_``: :obj:`dict`

        :param ``assert_status``: Desired status of Data object
        :type ``assert_status``: :obj:`str`

        :return: :obj:`server.models.Data` object which is created by
            the processor.

        """

        # backward compatibility
        process_slug = slugify(process_slug.replace(':', '-'))

        p = Process.objects.get(slug=process_slug)

        # since we don't know what uid/gid will be used inside Docker executor,
        # others must have all permissions on the upload directory
        upload_dir = settings.FLOW_EXECUTOR['UPLOAD_PATH']
        upload_dir_mode = stat.S_IMODE(os.stat(upload_dir).st_mode)
        others_all_perm = stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH
        if upload_dir_mode & others_all_perm != others_all_perm:
            raise ValueError("Incorrect permissions ({}) for upload dir ({}). "
                "Change it so that others will have read, write and execute "
                "permissions.".format(oct(upload_dir_mode), upload_dir))

        for field_schema, fields in iterate_fields(input_, p.input_schema):
            # copy referenced files to upload dir
            if field_schema['type'] == "basic:file:":
                for app_config in apps.get_app_configs():

                    old_path = os.path.join(app_config.path, 'tests', 'files', fields[field_schema['name']])
                    if os.path.isfile(old_path):
                        file_name = os.path.basename(fields[field_schema['name']])
                        new_path = os.path.join(upload_dir, file_name)
                        shutil.copy2(old_path, new_path)
                        # since we don't know what uid/gid will be used inside Docker executor,
                        # we must give others read and write permissions
                        os.chmod(new_path, 0o666)
                        fields[field_schema['name']] = {
                            'file': file_name,
                            'file_temp': file_name,
                        }
                        break

            # convert primary keys to strings
            if field_schema['type'].startswith('data:'):
                fields[field_schema['name']] = str(fields[field_schema['name']])
            if field_schema['type'].startswith('list:data:'):
                fields[field_schema['name']] = [str(obj) for obj in fields[field_schema['name']]]

        d = Data.objects.create(
            input=input_,
            contributor=self.admin,
            process=p,
            slug=get_random_string(length=6))
        self.project.data.add(d)

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
        :type obj: :obj:`server.models.Data`
        :param status: Data status to check
        :type status: str

        """
        self.assertEqual(obj.status, status,
                         msg="Data status is '{}', not '{}'".format(obj.status, status) + self._msg_out(obj))

    def assertFields(self, obj, path, value):  # pylint: disable=invalid-name
        """Compare Data object's field to given value.

        :param obj: Data object with field to compare
        :type obj: :obj:`server.models.Data`

        :param path: Path to field in Data object.
        :type path: :obj:`str`

        :param value: Desired value.
        :type value: :obj:`str`

        """
        field = dict_dot(obj['output'], path)
        self.assertEqual(field, value,
                         msg="Field 'output.{}' mismatch: {} != {}".format(path, field, str(value)) +
                         self._msg_out(obj))

    def assertFiles(self, obj, field_path, fn, compression=None):  # pylint: disable=invalid-name
        """Compare output file of a processor to the given correct file.

        :param obj: Data object which includes file that we want to
            compare.
        :type obj: :obj:`server.models.Data`

        :param field_path: Path to file name in Data object.
        :type field_path: :obj:`str`

        :param fn: File name (and relative path) of file to which we
            want to compare. Name/path is relative to
            'server/tests/processor/outputs'.
        :type fn: :obj:`str`

        :param compression: If not None, files will be uncompressed with
            the appropriate compression library before comparison.
            Currently supported compression formats are "gzip" and
            "zip".
        :type compression: :obj:`str`

        """
        if compression is None:
            open_fn = open
        elif compression == 'gzip':
            open_fn = gzip.open
        elif compression == 'zip':
            open_fn = zipfile.ZipFile.open
        else:
            raise ValueError("Unsupported compression format.")

        field = dict_dot(obj['output'], field_path)
        output = os.path.join(settings.FLOW_EXECUTOR['DATA_PATH'], str(obj.pk), field['file'])
        output_file = open_fn(output)
        output_hash = hashlib.sha256(output_file.read()).hexdigest()

        wanted = os.path.join(self.current_path, 'outputs', fn)

        if not os.path.isfile(wanted):
            shutil.copyfile(output, wanted)
            self.fail(msg="Output file {} missing so it was created.".format(fn))

        wanted_file = open_fn(wanted)
        wanted_hash = hashlib.sha256(wanted_file.read()).hexdigest()
        self.assertEqual(wanted_hash, output_hash,
                         msg="File hash mismatch: {} != {}".format(wanted_hash, output_hash) + self._msg_out(obj))

    def assertFileExist(self, obj, field_path):  # pylint: disable=invalid-name
        """Compare output file of a processor to the given correct file.

        :param obj: Data object which includes file that we want to
            compare.
        :type obj: :obj:`server.models.Data`

        :param field_path: Path to file name in Data object.
        :type field_path: :obj:`str`

        """
        field = dict_dot(obj['output'], field_path)
        output = os.path.join(settings.FLOW_EXECUTOR['DATA_PATH'], str(obj.pk), field['file'])

        if not os.path.isfile(output):
            self.fail(msg="File {} does not exist.".format(field_path))

    def assertJSON(self, obj, storage, field_path, file_name):  # pylint: disable=invalid-name
        """Compare JSON in Storage object to the given correct output.

        :param obj: Data object which includes file that we want to
            compare.
        :type obj: :obj:`server.models.Data`

        :param storage: Storage (or storage id) which contains JSON to
            compare.
        :type storage: :obj:`server.models.Storage` or :obj:`str`

        :param field_path: Path to JSON subset to compare in Storage
            object. If it is empty, entire Storage object will be
            compared.
        :type field_path: :obj:`str`

        :param file_name: File name (and relative path) of file to which we
            want to compare. Name/path is relative to
            'server/tests/processor/outputs'.
        :type file_name: :obj:`str`

        """
        self.assertEqual(os.path.splitext(file_name)[1], '.gz', msg='File extension must be .gz')

        if not isinstance(storage, Storage):
            storage = Storage.objects.get(pk=storage)

        storage_obj = dict_dot(storage['json'], field_path)

        file_path = os.path.join(self.current_path, 'outputs', file_name)
        if not os.path.isfile(file_path):
            with gzip.open(file_path, 'w') as f:
                json.dump(storage_obj, f)

            self.fail(msg="Output file {} missing so it was created.".format(file_name))

        with gzip.open(file_path) as f:
            file_obj = json.load(f)

        self.assertEqual(storage_obj, file_obj,
                         msg="Storage {} field '{}' does not match file {}".format(
                             storage.id, field_path, file_name) + self._msg_out(obj))

    def _msg_out(self, data):
        """Print stdout.txt's content."""
        msg = "\n\nDump stdout.txt:\n\n"
        path = os.path.join(settings.FLOW_EXECUTOR['DATA_PATH'], str(data.pk), "stdout.txt")
        if os.path.isfile(path):
            with open(path, 'r') as fn:
                msg += fn.read()

        return msg
