# pylint: disable=missing-docstring
import logging
from time import time
from unittest.mock import MagicMock, patch

from django.core.exceptions import ValidationError

from rest_framework.response import Response

from resolwe.flow.managers.workload_connectors.kubernetes import (
    sanitize_kubernetes_label,
)
from resolwe.flow.models import Data, Process
from resolwe.flow.utils import get_data_checksum, retry
from resolwe.flow.utils.exceptions import resolwe_exception_handler
from resolwe.test import TestCase


class ExceptionsTestCase(TestCase):
    @patch("resolwe.flow.utils.exceptions.exception_handler")
    def test_exception_handler(self, exception_handler_mock):
        exception_handler_mock.return_value = None
        resp = resolwe_exception_handler(Exception, {})
        self.assertEqual(resp, None)

        exception_handler_mock.return_value = None
        resp = resolwe_exception_handler(ValidationError("Error description"), {})
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.data["error"], "Error description")

        original_resp = Response({})
        exception_handler_mock.return_value = original_resp
        resp = resolwe_exception_handler(ValidationError("Error description"), {})
        self.assertEqual(id(original_resp), id(resp))
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.data["error"], "Error description")

    def test_checksum_consistency(self):
        process = Process(version="1.0.0", slug="my-process")
        data = Data()

        data.input = {"tss": 0, "genome": "HG19"}
        checksum = get_data_checksum(data.input, process.slug, process.version)
        self.assertEqual(
            checksum, "ca322c2bb48b58eea3946e624fe6cfdc53c2cc12478465b6f0ca2d722e280c4c"
        )

        data.input = {"genome": "HG19", "tss": 0}
        checksum = get_data_checksum(data.input, process.slug, process.version)
        self.assertEqual(
            checksum, "ca322c2bb48b58eea3946e624fe6cfdc53c2cc12478465b6f0ca2d722e280c4c"
        )


class RetryTestCase(TestCase):
    """Test the retry decorator."""

    def test_retry_decorator(self):
        logger = logging.getLogger(__name__)
        callback = MagicMock()

        @retry(logger=logger, min_sleep=0.01, max_sleep=0.1, max_retries=3)
        def retry_ok():
            self.count += 1

        @retry(logger=logger, min_sleep=0.01, max_sleep=0.1, max_retries=3)
        def retry_one():
            self.count += 1
            if self.count < 2:
                raise Exception("Error")

        @retry(
            logger=logger,
            min_sleep=0.01,
            max_sleep=0.1,
            max_retries=3,
            cleanup_callback=callback,
        )
        def retry_one_callback():
            self.count += 1
            if self.count < 2:
                raise Exception("Error")

        @retry(logger=logger, min_sleep=0.01, max_sleep=0.1, max_retries=3)
        def retry_two():
            self.count += 1
            if self.count < 3:
                raise Exception("Error")

        @retry(logger=logger, min_sleep=0.01, max_sleep=0.07, max_retries=5)
        def retry_fail():
            self.count += 1
            raise Exception("Error")

        # Test method that does not raise exception.
        # Assert that:
        # - count is set to 1
        # - no message is logged
        self.count = 0
        with self.assertRaises(AssertionError):
            with self.assertLogs(logger):
                retry_ok()
        self.assertEqual(self.count, 1)

        # Test method that raises single exception.
        # Assert that:
        # - count is set to 2
        # - single message is logged
        self.count = 0
        start = time()
        with self.assertLogs(logger) as cm:
            retry_one()
        end = time()
        self.assertEqual(self.count, 2)
        self.assertGreater(end - start, 0.01)
        expected_logger_output = [
            "ERROR:resolwe.flow.tests.test_utils:Retry 1/3 got exception, will retry in 0.01 seconds.\nTraceback"
        ]
        self.assertEqual(len(cm.output), len(expected_logger_output))
        for i in range(len(expected_logger_output)):
            self.assertIn(expected_logger_output[i], cm.output[i])

        # Test method that raises single exception with cleanup callback.
        # Assert that:
        # - count is set to 2
        # - single message is logged
        # - callback was called once
        self.count = 0
        start = time()
        with self.assertLogs(logger) as cm:
            retry_one_callback()
        end = time()
        self.assertEqual(self.count, 2)
        self.assertGreater(end - start, 0.01)
        expected_logger_output = [
            "ERROR:resolwe.flow.tests.test_utils:Retry 1/3 got exception, will retry in 0.01 seconds.\nTraceback"
        ]
        self.assertEqual(len(cm.output), len(expected_logger_output))
        for i in range(len(expected_logger_output)):
            self.assertIn(expected_logger_output[i], cm.output[i])
        callback.assert_called_once()

        # Test method that raises two exceptions.
        # Assert that:
        # - count is set to 3
        # - two messages are logged
        # - the second sleep is 2 times the first.
        self.count = 0
        start = time()
        with self.assertLogs(logger) as cm:
            retry_two()
        end = time()
        self.assertEqual(self.count, 3)
        self.assertGreater(end - start, 0.03)
        expected_logger_output = [
            "ERROR:resolwe.flow.tests.test_utils:Retry 1/3 got exception, will retry in 0.01 seconds.\nTraceback",
            "ERROR:resolwe.flow.tests.test_utils:Retry 2/3 got exception, will retry in 0.02 seconds.\nTraceback",
        ]
        self.assertEqual(len(cm.output), len(expected_logger_output))
        for i in range(len(expected_logger_output)):
            self.assertIn(expected_logger_output[i], cm.output[i])

        # Test method that always raises exception. The following is checked:
        # - exception should be raised
        # - counter should be set to the max_retries
        # - last log message should be different
        # - sleeps should be exponential
        # - max_sleep must be upper limit for sleeps
        self.count = 0
        with self.assertLogs(logger) as cm:
            with self.assertRaises(Exception):
                start = time()
                retry_fail()
        end = time()
        self.assertGreater(end - start, 0.14)
        self.assertLess(end - start, 0.2)
        self.assertEqual(self.count, 5)
        expected_logger_output = [
            "ERROR:resolwe.flow.tests.test_utils:Retry 1/5 got exception, will retry in 0.01 seconds.\nTraceback",
            "ERROR:resolwe.flow.tests.test_utils:Retry 2/5 got exception, will retry in 0.02 seconds.\nTraceback",
            "ERROR:resolwe.flow.tests.test_utils:Retry 3/5 got exception, will retry in 0.04 seconds.\nTraceback",
            "ERROR:resolwe.flow.tests.test_utils:Retry 4/5 got exception, will retry in 0.07 seconds.\nTraceback",
            "ERROR:resolwe.flow.tests.test_utils:Retry 5/5 got exception, re-raising it.\nTraceback",
        ]
        self.assertEqual(len(cm.output), len(expected_logger_output))
        for i in range(len(expected_logger_output)):
            self.assertIn(expected_logger_output[i], cm.output[i])


class KubernetesTestCase(TestCase):
    def test_kubernetes_label_sanitizer(self):
        label = "this-is-a-valid-label"
        self.assertEqual(label, sanitize_kubernetes_label(label))

        too_long_label = "this-is-a-valid-label" * 10
        self.assertEqual(
            too_long_label[-63:], sanitize_kubernetes_label(too_long_label)
        )
        weird_label = "invalid/label with .some*weird'characters"
        self.assertEqual(
            "invalid-label-with-some-weird-characters",
            sanitize_kubernetes_label(weird_label),
        )

        label = "*/.2must start and end with alphanumeric character_"
        self.assertEqual(
            "2must-start-and-end-with-alphanumeric-character",
            sanitize_kubernetes_label(label),
        )

        label = "_" * 100 + "I am too long"
        self.assertEqual("I-am-too-long", sanitize_kubernetes_label(label))
