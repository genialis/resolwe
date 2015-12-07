# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import mock
import unittest

from resolwe.flow.exprengines.dtlbash import ExpressionEngine


class ExprengineTestCase(unittest.TestCase):
    def test_invalid_template(self):
        data_mock = mock.MagicMock(process_error=[])
        data_mock.process.run = {'bash': '{% if reads.type.startswith("data:reads:") %}'}

        ExpressionEngine().eval(data_mock)

        self.assertTrue('Error in process script' in data_mock.process_error[0])


class CeleryEngineTestCase(unittest.TestCase):
    def test_passed_to_celery(self):
        pass
