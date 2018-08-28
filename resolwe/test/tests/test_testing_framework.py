# pylint: disable=missing-docstring
import builtins
import gzip
import io
import json
import os.path
import tempfile
import unittest.mock as mock

from resolwe.flow.models import Storage
from resolwe.test import ProcessTestCase, TestCase, is_testing, tag_process

PROCESSES_DIR = os.path.join(os.path.dirname(__file__), 'processes')
TEST_FILES_DIR = os.path.join(os.path.dirname(__file__), 'files')


class TestingFrameworkTestCase(TestCase):

    def setUp(self):
        self.dummy_case = ProcessTestCase.__new__(ProcessTestCase)
        self.dummy_case.files_path = ''
        self.dummy_case._debug_info = lambda _: ''  # pylint: disable=protected-access
        setattr(self.dummy_case, 'assertEqual', self.assertEqual)

    @mock.patch("os.path.isfile")
    def test_assert_file_date_nofilter(self, isfile_mock):
        isfile_mock.return_value = True
        output1_file = io.BytesIO(b"some line\ndate: 2016-02-10\n")
        output2_file = io.BytesIO(b"some line\ndate: 2015-10-31\n")
        open_mock = mock.MagicMock(side_effect=[output1_file, output2_file])
        with mock.patch.object(builtins, 'open', open_mock):
            obj_mock = mock.MagicMock()
            self.assertRaises(
                AssertionError,
                ProcessTestCase._assert_file,  # pylint: disable=protected-access
                self.dummy_case, obj_mock, "", "",
            )

    @mock.patch("os.path.isfile")
    def test_assert_file_date_filter(self, isfile_mock):
        isfile_mock.return_value = True
        output1_file = io.BytesIO(b"some line\ndate: 2016-02-10\n")
        output2_file = io.BytesIO(b"some line\ndate: 2015-10-31\n")
        open_mock = mock.MagicMock(side_effect=[output1_file, output2_file])
        with mock.patch.object(builtins, 'open', open_mock):
            obj_mock = mock.MagicMock()

            def date_in_line(line):
                return line.startswith(b"date")

            ProcessTestCase._assert_file(  # pylint: disable=protected-access
                self.dummy_case, obj_mock, "", "", file_filter=date_in_line)

    @mock.patch("os.path.isfile")
    def test_assert_file_sort_false(self, isfile_mock):
        isfile_mock.return_value = True
        output1_file = io.BytesIO(b"A\nB\nC\n")
        output2_file = io.BytesIO(b"A\nC\nB\n")
        open_mock = mock.MagicMock(side_effect=[output1_file, output2_file])
        with mock.patch.object(builtins, 'open', open_mock):
            obj_mock = mock.MagicMock()
            self.assertRaises(
                AssertionError,
                ProcessTestCase._assert_file,  # pylint: disable=protected-access
                self.dummy_case, obj_mock, "", "",
            )

    @mock.patch("os.path.isfile")
    def test_assert_file_sort_true(self, isfile_mock):
        isfile_mock.return_value = True
        output1_file = io.BytesIO(b"A\nB\nC\n")
        output2_file = io.BytesIO(b"A\nC\nB\n")
        open_mock = mock.MagicMock(side_effect=[output1_file, output2_file])
        with mock.patch.object(builtins, 'open', open_mock):
            obj_mock = mock.MagicMock()
            ProcessTestCase._assert_file(  # pylint: disable=protected-access
                self.dummy_case, obj_mock, "", "", sort=True)

    def test_assert_json_storage_object(self):
        example_json = {'foo': [1.0, 2.5, 3.14], 'bar': ['ba', 'cd']}

        obj_mock = mock.MagicMock()

        storage_mock = mock.MagicMock(spec=Storage)
        storage_mock.id = 'no_id'
        storage_mock.json = example_json

        # use in-memory binary stream object for speed and simplicity
        gzipped_json_file = io.BytesIO()
        with gzip.open(gzipped_json_file, mode='wt') as f:
            json.dump(example_json, f)
        # set seek position of the binary stream object back to 0
        gzipped_json_file.seek(0)

        join_mock = mock.MagicMock(side_effect=[gzipped_json_file])
        isfile_mock = mock.MagicMock(return_value=True)
        with mock.patch.object(os.path, 'join', join_mock):
            with mock.patch.object(os.path, 'isfile', isfile_mock):
                ProcessTestCase.assertJSON(self.dummy_case, obj_mock, storage_mock, '', 'foo.gz')

    @mock.patch('resolwe.flow.models.Storage.objects.get')
    def test_assert_json_storage_id(self, get_mock):
        example_json = {'foo': [1.0, 2.5, 3.14], 'bar': ['ba', 'cd']}

        obj_mock = mock.MagicMock()

        storage_id = 'no_id'
        storage_mock = mock.MagicMock(spec=Storage)
        storage_mock.id = storage_id
        storage_mock.json = example_json
        get_mock.side_effect = [storage_mock]

        # use in-memory binary stream object for speed and simplicity
        gzipped_json_file = io.BytesIO()
        with gzip.open(gzipped_json_file, mode='wt') as f:
            json.dump(example_json, f)
        # set seek position of the binary stream object back to 0
        gzipped_json_file.seek(0)

        join_mock = mock.MagicMock(side_effect=[gzipped_json_file])
        isfile_mock = mock.MagicMock(return_value=True)
        with mock.patch.object(os.path, 'join', join_mock):
            with mock.patch.object(os.path, 'isfile', isfile_mock):
                ProcessTestCase.assertJSON(self.dummy_case, obj_mock, storage_id, '', 'foo.gz')

    def test_assert_json_file_missing(self):
        example_json = {'foo': [1.0, 2.5, 3.14], 'bar': ['ba', 'cd']}

        obj_mock = mock.MagicMock()

        storage_mock = mock.MagicMock(spec=Storage)
        storage_mock.id = 'no_id'
        storage_mock.json = example_json

        # use in-memory binary stream object for speed and simplicity
        gzipped_json_file = io.BytesIO()
        join_modified_values = [gzipped_json_file]
        orig_join = os.path.join

        # NOTE: coverage tool needs original 'os.path.join'
        def join_side_effect(path, *paths):
            if join_modified_values:
                return join_modified_values.pop(0)
            else:
                return orig_join(path, *paths)

        join_mock = mock.MagicMock(side_effect=join_side_effect)
        isfile_mock = mock.MagicMock(return_value=False)
        with mock.patch.object(os.path, 'join', join_mock):
            with mock.patch.object(os.path, 'isfile', isfile_mock):

                # https://github.com/PyCQA/pylint/issues/1653
                self.assertRaisesRegex(  # pylint: disable=deprecated-method
                    AssertionError,
                    'Output file .* missing so it was created.',
                    ProcessTestCase.assertJSON,
                    self.dummy_case, obj_mock, storage_mock, '', 'foo.gz',
                )

        # set seek position of the binary stream object back to 0
        gzipped_json_file.seek(0)
        with gzip.open(gzipped_json_file, mode='rt') as f:
            unzipped_json = json.load(f)
        self.assertEqual(example_json, unzipped_json)

    def test_debug_info_non_ascii(self):
        non_ascii_text = 'Some non-ascii chars č ü €'

        dummy_case = ProcessTestCase.__new__(ProcessTestCase)

        obj_mock = mock.MagicMock()
        obj_mock.pk = 'no_id'

        with tempfile.NamedTemporaryFile(mode='wb') as stdout_file:
            stdout_file.write(non_ascii_text.encode('utf-8'))
            # set seek position of the file back to 0
            stdout_file.seek(0)
            with mock.patch.object(os.path, 'join', side_effect=[stdout_file.name]):
                with mock.patch.object(os.path, 'isfile', return_value=True):

                    # https://github.com/PyCQA/pylint/issues/1653
                    self.assertRegex(  # pylint: disable=deprecated-method
                        ProcessTestCase._debug_info(dummy_case, obj_mock),   # pylint: disable=protected-access
                        non_ascii_text,
                    )

    def test_assert_almost_equal_generic(self):  # pylint: disable=invalid-name
        self.assertAlmostEqualGeneric(1.00000001, 1.0)
        self.assertAlmostEqualGeneric([1.00000001], [1.0])
        self.assertAlmostEqualGeneric({'foo': 1.00000001}, {'foo': 1.0})
        self.assertAlmostEqualGeneric({'foo': [1.00000001]}, {'foo': [1.0]})
        self.assertAlmostEqualGeneric({'foo': 1.00000001, 'bar': 'moo'}, {'foo': 1.0, 'bar': 'moo'})
        self.assertAlmostEqualGeneric(1.00000001, 1.0, msg="Test message")

    def test_is_testing(self):
        self.assertTrue(is_testing())


class TestingFrameworkProcessTestCase(ProcessTestCase):
    def setUp(self):
        super().setUp()
        self._register_schemas(path=[PROCESSES_DIR])

    @tag_process('test-file-upload')
    def test_url_file(self):
        url = 'https://www.genialis.com/data.csv.gz'
        inputs = {
            'src': url,
        }
        datum = self.run_process('test-file-upload', inputs)
        datum.refresh_from_db()

        self.assertFields(datum, 'file_temp', url)
        self.assertFields(datum, 'file', url)

    @tag_process('test-file-upload')
    def test_do_not_mutate_inputs(self):
        inputs = {
            'src': 'example_file.txt',
        }
        original_inputs = inputs.copy()

        self.files_path = TEST_FILES_DIR
        self.run_process('test-file-upload', inputs)

        self.assertEqual(original_inputs, inputs)
