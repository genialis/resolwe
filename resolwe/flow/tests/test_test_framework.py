# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import gzip
import io
import json
import mock
import os.path
import sys
import unittest

try:
    import builtins  # py3
except ImportError:
    import __builtin__ as builtins  # py2

import six

from resolwe.flow.models import Storage
from resolwe.flow.utils.test import ProcessTestCase


class TestingFrameworkTestCase(unittest.TestCase):

        @mock.patch("os.path.isfile")
        @mock.patch("resolwe.flow.utils.test.dict_dot")
        def test_assert_files_date_not_filtered(self, dict_dot_mock, isfile_mock):
            isfile_mock.return_value = True
            output1_file = io.BytesIO(b"some line\ndate: 2016-02-10\n")
            output2_file = io.BytesIO(b"some line\ndate: 2015-10-31\n")
            open_mock = mock.MagicMock(side_effect=[output1_file, output2_file])
            with mock.patch.object(builtins, 'open', open_mock):
                dummy_case = ProcessTestCase.__new__(ProcessTestCase)
                dummy_case.files_path = ""
                dummy_case._debug_info = lambda _: ""
                dummy_case.assertEqual = self.assertEqual
                obj_mock = mock.MagicMock()
                self.assertRaises(AssertionError, ProcessTestCase._assert_file,
                                  dummy_case, obj_mock, "", "")

        @mock.patch("os.path.isfile")
        @mock.patch("resolwe.flow.utils.test.dict_dot")
        def test_assert_files_date_filtered(self, dict_dot_mock, isfile_mock):
            isfile_mock.return_value = True
            output1_file = io.BytesIO(b"some line\ndate: 2016-02-10\n")
            output2_file = io.BytesIO(b"some line\ndate: 2015-10-31\n")
            open_mock = mock.MagicMock(side_effect=[output1_file, output2_file])
            with mock.patch.object(builtins, 'open', open_mock):
                dummy_case = ProcessTestCase.__new__(ProcessTestCase)
                dummy_case.files_path = ""
                dummy_case._debug_info = lambda _: ""
                dummy_case.assertEqual = self.assertEqual
                obj_mock = mock.MagicMock()

                def date_in_line(x):
                    return x.startswith(b"date")

                ProcessTestCase._assert_file(dummy_case, obj_mock, "", "", filter=date_in_line)

        @unittest.skipIf(six.PY2, "Needs Python 3 version of the gzip module")
        def test_assert_json_storage_object(self):
            example_json = {'foo': [1.0, 2.5, 3.14], 'bar': ['ba', 'cd']}

            dummy_case = ProcessTestCase.__new__(ProcessTestCase)
            dummy_case.files_path = ''
            dummy_case._debug_info = lambda _: ''
            dummy_case.assertEqual = self.assertEqual

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
                    ProcessTestCase.assertJSON(dummy_case, obj_mock, storage_mock, '', 'foo.gz')

        @unittest.skipIf(six.PY2, "Needs Python 3 version of the gzip module")
        @mock.patch('resolwe.flow.models.Storage.objects.get')
        def test_assert_json_storage_id(self, get_mock):
            example_json = {'foo': [1.0, 2.5, 3.14], 'bar': ['ba', 'cd']}

            dummy_case = ProcessTestCase.__new__(ProcessTestCase)
            dummy_case.files_path = ''
            dummy_case._debug_info = lambda _: ''
            dummy_case.assertEqual = self.assertEqual

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
                    ProcessTestCase.assertJSON(dummy_case, obj_mock, storage_id, '', 'foo.gz')

        @unittest.skipIf(six.PY2, "Needs Python 3 version of the gzip module")
        def test_assert_json_file_missing(self):
            example_json = {'foo': [1.0, 2.5, 3.14], 'bar': ['ba', 'cd']}

            dummy_case = ProcessTestCase.__new__(ProcessTestCase)
            dummy_case.files_path = ''
            dummy_case._debug_info = lambda _: ''
            dummy_case.assertEqual = self.assertEqual

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
                if len(join_modified_values) > 0:
                    return join_modified_values.pop(0)
                else:
                    return orig_join(path, *paths)

            join_mock = mock.MagicMock(side_effect=join_side_effect)
            isfile_mock = mock.MagicMock(return_value=False)
            with mock.patch.object(os.path, 'join', join_mock):
                with mock.patch.object(os.path, 'isfile', isfile_mock):
                    with six.assertRaisesRegex(self, AssertionError,
                                               'Output file .* missing so it was created.'):
                        ProcessTestCase.assertJSON(dummy_case, obj_mock, storage_mock, '', 'foo.gz')

            # set seek position of the binary stream object back to 0
            gzipped_json_file.seek(0)
            with gzip.open(gzipped_json_file, mode='rt') as f:
                unzipped_json = json.load(f)
            self.assertEqual(example_json, unzipped_json)
