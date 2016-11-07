# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from mock import patch

from django.core.exceptions import ValidationError
from django.test import TestCase

from rest_framework.response import Response

from resolwe.flow.utils.exceptions import resolwe_exception_handler


class ExceptionsTestCase(TestCase):

    @patch('resolwe.flow.utils.exceptions.exception_handler')
    def test_exception_handler(self, exception_handler_mock):
        exception_handler_mock.return_value = None
        resp = resolwe_exception_handler(Exception, {})
        self.assertEqual(resp, None)

        exception_handler_mock.return_value = None
        resp = resolwe_exception_handler(ValidationError('Error description'), {})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.data['error'], 'Error description')

        original_resp = Response({})
        exception_handler_mock.return_value = original_resp
        resp = resolwe_exception_handler(ValidationError('Error description'), {})
        self.assertEqual(id(original_resp), id(resp))
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.data['error'], 'Error description')
