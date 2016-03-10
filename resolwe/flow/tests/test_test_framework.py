# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import io
import mock
import unittest

try:
    import builtins  # py3
except ImportError:
    import __builtin__ as builtins  # py2

from resolwe.flow.tests import ProcessTestCase


class TestingFrameworkTestCase(unittest.TestCase):

        @mock.patch("os.path.isfile")
        @mock.patch("resolwe.flow.tests.dict_dot")
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
                self.assertRaises(AssertionError, ProcessTestCase.assertFiles,
                                  dummy_case, obj_mock, "", "")

        @mock.patch("os.path.isfile")
        @mock.patch("resolwe.flow.tests.dict_dot")
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
                ProcessTestCase.assertFiles(dummy_case, obj_mock, "", "", filter=date_in_line)


