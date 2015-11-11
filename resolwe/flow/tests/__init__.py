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
import zipfile

from django.conf import settings
from django.core import management
from django.test import TestCase
from django.contrib.auth import get_user_model

from resolwe.flow.models import Data, dict_dot, Process, Project, Storage


PROCESSORS_FIXTURE_CACHE = None


def _register_processors():
    """Register processors.

    Processor definitions are red when the first test is callled and cached
    into the PROCESSORS_FIXTURE_CACHE global variable.

    """
    Process.objects.all().delete()

    global PROCESSORS_FIXTURE_CACHE  # pylint: disable=global-statement
    if PROCESSORS_FIXTURE_CACHE:
        Process.objects.bulk_create(PROCESSORS_FIXTURE_CACHE)
    else:
        user_model = get_user_model()

        if not user_model.objects.filter(is_superuser=True).exists():
            user_model.objects.create_superuser(username="admin", email='admin@example.com', password="admin_pass")

        management.call_command('register', force=True, testing=True, verbosity='0')
        PROCESSORS_FIXTURE_CACHE = list(Process.objects.all())  # list forces db query execution


class BaseProcessorTestCase(TestCase):

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
        super(BaseProcessorTestCase, self).setUp()
        self.admin = get_user_model().objects.create_superuser(
            username="admin", email='admin@example.com', password="admin_pass")
        _register_processors()

        self.project = Project.objects.create(contributor=self.admin)
        self.current_path = os.path.dirname(os.path.abspath(__file__))
        self._keep_all = False
        self._keep_failed = False

    def tearDown(self):
        super(BaseProcessorTestCase, self).tearDown()

        # Delete Data objects and their files unless keep_failed
        for d in Data.objects.all():
            if self._keep_all or (self._keep_failed and d.status == "error"):
                print("KEEPING DATA: {}".format(d.pk))
            else:
                data_dir = os.path.join(settings.DATAFS['data_path'], str(d.pk))
                d.delete()
                shutil.rmtree(data_dir, ignore_errors=True)

    def keep_all(self):
        """Do not delete output files after test for all data."""
        self._keep_all = True

    def keep_failed(self):
        """Do not delete output files after test for failed data."""
        self._keep_failed = True

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
        output = os.path.join(settings.DATAFS['data_path'], str(obj.pk), field['file'])
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
        output = os.path.join(settings.DATAFS['data_path'], str(obj.pk), field['file'])

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
        """Print stdout.txt and stderr.txt content."""
        msg = ''
        for fn in ['stdout.txt', 'stderr.txt']:
            msg += "\n\nDump {}:\n\n".format(fn)
            path = os.path.join(settings.FLOW_EXECUTOR['DATA_PATH'], str(data.pk), fn)
            if os.path.isfile(path):
                with open(path, 'r') as fn:
                    msg += fn.read()

        return msg
