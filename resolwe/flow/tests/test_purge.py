# pylint: disable=missing-docstring
import contextlib
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from testfixtures import LogCapture

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import override_settings
from django.utils.crypto import get_random_string

from resolwe.flow.managers.utils import disable_auto_calls
from resolwe.flow.models import Data, DataLocation, DescriptorSchema, Process, Storage
from resolwe.flow.utils import purge
from resolwe.test import ProcessTestCase


class PurgeTestFieldsMixin:
    """
    This class contains tests, which validate each field individually. It is used
    to perform basic unit tests (on simulated data, without running any processors)
    as well as more complex end-to-end tests (by running processors). Having them
    in this mixin makes sure that both, unit and e2e tests, are run for each field.
    """

    def test_basic_file(self):
        self.assertFieldWorks(
            'basic:file:',
            field_value={'file': 'but_not_this'},
            script_setup='touch remove_this but_not_this',
            script_save='re-save-file sample but_not_this',
            removed=['remove_this'],
            not_removed=['but_not_this'],
        )

    def test_basic_file_specialization(self):
        self.assertFieldWorks(
            'basic:file:html:',
            field_value={'file': 'but_not_this'},
            script_setup='touch remove_this but_not_this',
            script_save='re-save-file sample but_not_this',
            removed=['remove_this'],
            not_removed=['but_not_this'],
        )

    def test_basic_file_list(self):
        self.assertFieldWorks(
            'list:basic:file:',
            field_value=[{'file': 'but_not_this'}, {'file': 'and_not_this'}],
            script_setup='touch remove_this but_not_this and_not_this',
            script_save='re-save-file-list sample but_not_this and_not_this',
            removed=['remove_this'],
            not_removed=['but_not_this', 'and_not_this'],
        )

    def test_basic_dir(self):
        self.assertFieldWorks(
            'basic:dir:',
            field_value={'dir': 'but_not_this'},
            script_setup="""
mkdir remove_this but_not_this
touch remove_this/a remove_this/b remove_this/c
touch but_not_this/a but_not_this/b but_not_this/c
""",
            script_save='re-save-dir sample but_not_this',
            removed=['remove_this/', 'remove_this/a', 'remove_this/b', 'remove_this/c'],
            not_removed=['but_not_this/a', 'but_not_this/b', 'but_not_this/c'],
        )

    def test_basic_dir_list(self):
        self.assertFieldWorks(
            'list:basic:dir:',
            field_value=[{'dir': 'but_not_this'}, {'dir': 'and_not_this'}],
            script_setup="""
mkdir remove_this but_not_this and_not_this
touch remove_this/a remove_this/b remove_this/c
touch but_not_this/a but_not_this/b but_not_this/c
touch and_not_this/a and_not_this/b and_not_this/c
""",
            script_save='re-save-dir-list sample but_not_this and_not_this',
            removed=['remove_this/', 'remove_this/a', 'remove_this/b', 'remove_this/c'],
            not_removed=['but_not_this/a', 'but_not_this/b', 'but_not_this/c', 'and_not_this/a',
                         'and_not_this/b', 'and_not_this/c'],
        )


@override_settings(TEST_PROCESS_REQUIRE_TAGS=False)  # Test uses dynamic processes.
class PurgeE2ETest(PurgeTestFieldsMixin, ProcessTestCase):

    def create_and_run_processor(self, processor, **kwargs):
        processor_slug = get_random_string(6)
        Process.objects.create(
            slug=processor_slug,
            name='Test Purge Process',
            contributor=self.admin,
            type='data:test',
            version=1,
            **processor
        )

        data = self.run_process(processor_slug, **kwargs)
        # Purge is normally called in an async worker, so we have to emulate the call.
        purge.location_purge(location_id=data.location.id, delete=True)

        return data

    def assertFilesRemoved(self, data, *files):  # pylint: disable=invalid-name
        for name in files:
            path = data.location.get_path(filename=name)
            self.assertFalse(os.path.isfile(path), msg=path)

    def assertFilesNotRemoved(self, data, *files):  # pylint: disable=invalid-name
        for name in files:
            path = data.location.get_path(filename=name)
            self.assertTrue(os.path.isfile(path), msg=path)

    def test_complex_processor(self):
        data = self.create_and_run_processor(
            processor=dict(
                input_schema=[],
                output_schema=[
                    {
                        'name': 'sample_file',
                        'label': 'Sample output file',
                        'type': 'basic:file:'
                    },
                    {
                        'name': 'sample_dir',
                        'label': 'Sample output directory',
                        'type': 'basic:dir:'
                    },
                    {
                        'name': 'sample_file_list',
                        'label': 'Sample list of output files',
                        'type': 'list:basic:file:'
                    },
                    {
                        'name': 'sample_dir_list',
                        'label': 'Sample list of output directories',
                        'type': 'list:basic:dir:'
                    }
                ],
                run={
                    'language': 'bash',
                    'program': """
touch these files should be removed
touch this_file_should_stay
mkdir -p directory/should/be
touch directory/should/be/removed
touch directory/should/be/removed2
mkdir -p stay/directory
touch stay/directory/file1
touch stay/directory/file2
touch entry1 entry2 entry3
mkdir dir1 dir2 dir3
touch dir1/a dir2/b dir3/c dir3/d
touch ref1 ref2 ref3 ref4
mkdir refs
touch refs/a refs/b

re-save-file sample_file this_file_should_stay ref3
re-save-dir sample_dir stay ref4
re-save-file-list sample_file_list entry1 entry2:ref1 entry3:refs
re-save-dir-list sample_dir_list dir1:ref2 dir2 dir3
""",
                }
            )
        )

        self.assertFilesRemoved(data, 'these', 'files', 'should', 'be', 'removed', 'directory')
        self.assertFilesNotRemoved(data, 'this_file_should_stay', 'stay/directory/file1', 'stay/directory/file2',
                                   'entry1', 'entry2', 'entry3', 'dir1/a', 'dir2/b', 'dir3/c', 'dir3/d', 'ref1',
                                   'ref2', 'ref3', 'ref4', 'refs/a', 'refs/b')

        data.location.refresh_from_db()
        self.assertEqual(data.location.purged, True)

    def assertFieldWorks(self, field_type, field_value, script_setup,  # pylint: disable=invalid-name
                         script_save, removed, not_removed):
        """
        Checks that a field is handled correctly when running a processor, which
        uses the field.
        """

        field_schema = {
            'name': 'sample',
            'label': 'Sample output',
            'type': field_type
        }

        # Test output.
        data = self.create_and_run_processor(
            processor=dict(
                input_schema=[],
                output_schema=[field_schema],
                run={
                    'language': 'bash',
                    'program': script_setup + '\n' + script_save
                }
            )
        )

        self.assertFilesRemoved(data, *removed)
        self.assertFilesNotRemoved(data, *not_removed)

        # Test descriptor.
        descriptor_schema = DescriptorSchema.objects.create(
            slug=get_random_string(6),
            contributor=self.admin,
            schema=[field_schema]
        )
        data = self.create_and_run_processor(
            processor=dict(
                input_schema=[],
                output_schema=[],
                run={
                    'language': 'bash',
                    'program': script_setup
                }
            ),
            descriptor_schema=descriptor_schema,
            descriptor={'sample': field_value}
        )

        self.assertFilesRemoved(data, *removed)
        self.assertFilesNotRemoved(data, *not_removed)

    @unittest.skipIf(True, "since PR308: manager now separated into parts, executor not logging here anymore")
    def test_exception_logging(self):
        with patch('resolwe.flow.utils.purge.os', wraps=os) as os_mock:
            # Ensure that purge raises an exception, so we can check whether the exception
            # gets logged correctly.
            class TestPurgeException(Exception):
                pass

            def exception_raiser(*args, **kwargs):
                raise TestPurgeException
            os_mock.walk = exception_raiser

            with LogCapture() as log:
                self.create_and_run_processor(
                    processor=dict(
                        input_schema=[],
                        output_schema=[{
                            'name': 'sample',
                            'label': 'Sample output',
                            'type': 'basic:file:'
                        }],
                        run={
                            'language': 'bash',
                            'program': 'touch sample && re-save-file sample sample'
                        }
                    )
                )

                self.assertEqual(len(log.records), 1)
                self.assertEqual(log.records[0].name, 'resolwe.flow.executors')
                self.assertTrue(str(log.records[0].msg).startswith('Purge error:'))
                self.assertTrue('TestPurgeException' in str(log.records[0].msg))


class PurgeUnitTest(PurgeTestFieldsMixin, ProcessTestCase):

    def assertFieldWorks(self, field_type, field_value, script_setup,  # pylint: disable=invalid-name
                         script_save, removed, not_removed):
        """
        Checks that a field is handled correctly by `get_purge_files` under a
        simulated Data object.
        """

        field_schema = {
            'name': 'sample',
            'label': 'Sample output',
            'type': field_type
        }

        # Test simulated operation.
        simulated_root = tempfile.mkdtemp()
        try:
            for filename in removed + not_removed:
                directory, basename = os.path.split(filename)
                if directory:
                    try:
                        os.makedirs(os.path.join(simulated_root, directory))
                    except OSError:
                        pass

                if basename:
                    with open(os.path.join(simulated_root, filename), 'w'):
                        pass

            unreferenced_files = purge.get_purge_files(
                simulated_root,
                output={'sample': field_value},
                output_schema=[field_schema],
                descriptor={},
                descriptor_schema=[]
            )

            def strip_slash(filename):
                return filename[:-1] if filename[-1] == '/' else filename

            for filename in not_removed:
                self.assertNotIn(strip_slash(os.path.join(simulated_root, filename)), unreferenced_files)

            for filename in removed:
                filename = strip_slash(os.path.join(simulated_root, filename))
                self.assertIn(filename, unreferenced_files)
                unreferenced_files.discard(filename)

            # Ensure that nothing more is removed.
            self.assertEqual(len(unreferenced_files), 0)
        finally:
            shutil.rmtree(simulated_root)

    def create_test_file(self, location, filename):
        """
        Creates an empty file in the proper directory, so that a file can be used
        as a reference in the Data object's output/descriptor.
        """
        root = location.get_path()
        try:
            os.makedirs(root)
            self.test_files.add(root)
        except OSError:
            pass

        filename = location.get_path(filename=filename)
        with open(filename, 'w'):
            self.test_files.add(filename)

    def setUp(self):
        super().setUp()

        self.test_files = set()

        self.user = get_user_model().objects.create(username="test_user")
        processor = Process.objects.create(
            name='Test process',
            contributor=self.user,
            output_schema=[
                {'name': 'sample', 'type': 'basic:file:'}
            ]
        )
        self.data = {
            'name': 'Test data',
            'contributor': self.user,
            'process': processor,
        }

    def tearDown(self):
        for filename in self.test_files:
            if os.path.isfile(filename):
                os.remove(filename)
            elif os.path.isdir(filename):
                shutil.rmtree(filename)

        super().tearDown()

    # This patch is required so that the manager is not invoked while saving Data.
    @disable_auto_calls()
    def test_remove(self):
        completed_data = Data.objects.create(**self.data)
        data_location = DataLocation.objects.create(subpath='')
        data_location.subpath = str(data_location.id)
        data_location.save()
        data_location.data.add(completed_data)
        completed_data.status = Data.STATUS_DONE
        completed_data.output = {'sample': {'file': 'test-file'}}
        self.create_test_file(completed_data.location, 'test-file')
        self.create_test_file(completed_data.location, 'removeme')
        completed_data.save()

        pending_data = Data.objects.create(**self.data)
        data_location = DataLocation.objects.create(subpath='')
        data_location.subpath = str(data_location.id)
        data_location.save()
        data_location.data.add(pending_data)
        self.create_test_file(pending_data.location, 'test-file')
        self.create_test_file(pending_data.location, 'donotremoveme')

        # Check that nothing is removed if delete is False (the default).
        with patch('resolwe.flow.utils.purge.os', wraps=os) as os_mock:
            os_mock.remove = MagicMock()
            purge.purge_all()
            os_mock.remove.assert_not_called()

        completed_data.location.purged = False
        completed_data.location.save()

        # Check that only the 'removeme' file from the completed Data objects is removed
        # and files from the second (not completed) Data objects are unchanged.
        with patch('resolwe.flow.utils.purge.os', wraps=os) as os_mock:
            os_mock.remove = MagicMock()
            purge.purge_all(delete=True)
            os_mock.remove.assert_called_once_with(
                completed_data.location.get_path(filename='removeme'))

        completed_data.location.purged = False
        completed_data.location.save()

        # Create dummy data directories for non-existant data objects.
        self.create_test_file(DataLocation.objects.create(subpath='990'), 'dummy')
        self.create_test_file(DataLocation.objects.create(subpath='991'), 'dummy')

        # Check that only the 'removeme' file from the completed Data objects is removed
        # together with directories not belonging to any data objects.
        with contextlib.ExitStack() as stack:
            os_mock = stack.enter_context(patch('resolwe.flow.utils.purge.os', wraps=os))
            shutil_mock = stack.enter_context(patch('resolwe.flow.utils.purge.shutil', wraps=shutil))

            os_mock.remove = MagicMock()
            shutil_mock.rmtree = MagicMock()
            purge.purge_all(delete=True)
            self.assertEqual(os_mock.remove.call_count, 1)
            self.assertEqual(shutil_mock.rmtree.call_count, 2)
            os_mock.remove.assert_called_once_with(
                os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(completed_data.location.id), 'removeme'))
            shutil_mock.rmtree.assert_any_call(
                os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], '990'))
            shutil_mock.rmtree.assert_any_call(
                os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], '991'))

        completed_data.location.purged = False
        completed_data.location.save()

        # Create another data object and check that if remove is called on one object,
        # only that object's data is removed.
        another_data = Data.objects.create(**self.data)
        data_location = DataLocation.objects.create(subpath='')
        data_location.subpath = str(data_location.id)
        data_location.save()
        data_location.data.add(another_data)
        another_data.status = Data.STATUS_DONE
        another_data.output = {'sample': {'file': 'test-file'}}
        self.create_test_file(another_data.location, 'test-file')
        self.create_test_file(another_data.location, 'removeme')
        another_data.save()

        with contextlib.ExitStack() as stack:
            os_mock = stack.enter_context(patch('resolwe.flow.utils.purge.os', wraps=os))
            shutil_mock = stack.enter_context(patch('resolwe.flow.utils.purge.shutil', wraps=shutil))

            os_mock.remove = MagicMock()
            purge.location_purge(location_id=another_data.location.id, delete=True)
            os_mock.remove.assert_called_once_with(
                another_data.location.get_path(filename='removeme'))
            shutil_mock.rmtree.assert_not_called()

    # This patch is required so that the manager is not invoked while saving Data.
    @disable_auto_calls()
    def test_remove_same_location(self):
        # Create two data objects that reference the same data location and check
        # that if one object is deleted, the data is not removed upon purge.
        # It should be removed only when location is not referenced by any data object.
        same_location_data = Data.objects.create(**self.data)
        data_location = DataLocation.objects.create(subpath='')
        data_location.subpath = str(data_location.id)
        data_location.save()
        data_location.data.add(same_location_data)
        subpath = same_location_data.location.subpath

        same_location_data.output = {'sample': {'file': 'test-file'}}
        same_location_data.status = Data.STATUS_DONE
        self.create_test_file(same_location_data.location, 'test-file')
        same_location_data.save()

        same_location_data_2 = Data.objects.create(**self.data)
        same_location_data.location.data.add(same_location_data_2)
        same_location_data_2.output = {'sample': {'file': 'test-file'}}
        same_location_data_2.status = Data.STATUS_DONE
        same_location_data_2.save()

        not_to_be_deleted = Data.objects.create(**self.data)
        data_location = DataLocation.objects.create(subpath='')
        data_location.subpath = str(data_location.id)
        data_location.save()
        data_location.data.add(not_to_be_deleted)
        not_to_be_deleted.output = {'sample': {'file': 'test-file'}}
        not_to_be_deleted.status = Data.STATUS_DONE
        self.create_test_file(not_to_be_deleted.location, 'test-file')
        not_to_be_deleted.save()

        self.assertEqual(Data.objects.count(), 3)

        # Delete first object.
        same_location_data.delete()

        self.assertEqual(Data.objects.count(), 2)
        with patch('resolwe.flow.utils.purge.shutil.rmtree', wraps=shutil.rmtree) as rmtree_mock:
            purge.purge_all(delete=True)
            rmtree_mock.assert_not_called()

        # Delete second object.
        same_location_data_2.delete()

        self.assertEqual(Data.objects.count(), 1)
        self.assertEqual(Data.objects.first().id, not_to_be_deleted.id)
        with patch('resolwe.flow.utils.purge.shutil.rmtree', wraps=shutil.rmtree) as rmtree_mock:
            purge.purge_all(delete=True)
            rmtree_mock.assert_called_once_with(
                os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], subpath))

    # This patch is required so that the manager is not invoked while saving Data.
    @disable_auto_calls()
    def test_remove_storage(self):
        Storage.objects.create(contributor=self.user, json={})
        Storage.objects.create(contributor=self.user, json={})

        data = Data.objects.create(**self.data)
        data.storages.add(Storage.objects.create(contributor=self.user, json={}))

        purge._storage_purge_all()  # pylint: disable=protected-access
        self.assertEqual(Storage.objects.count(), 3)

        purge._storage_purge_all(delete=True)  # pylint: disable=protected-access
        self.assertEqual(Storage.objects.count(), 1)
