# pylint: disable=missing-docstring
from mock import patch

from django.core.exceptions import ValidationError

from rest_framework.response import Response

from resolwe.flow.models import Data, Process
from resolwe.flow.utils import get_data_checksum
from resolwe.flow.utils.exceptions import resolwe_exception_handler
from resolwe.test import TestCase


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

    def test_checksum_consistency(self):
        process = Process(version='1.0.0', slug='my-process')
        data = Data()

        data.input = {'tss': 0, 'genome': 'HG19'}
        checksum = get_data_checksum(data.input, process.slug, process.version)
        self.assertEqual(checksum, 'ca322c2bb48b58eea3946e624fe6cfdc53c2cc12478465b6f0ca2d722e280c4c')

        data.input = {'genome': 'HG19', 'tss': 0}
        checksum = get_data_checksum(data.input, process.slug, process.version)
        self.assertEqual(checksum, 'ca322c2bb48b58eea3946e624fe6cfdc53c2cc12478465b6f0ca2d722e280c4c')
