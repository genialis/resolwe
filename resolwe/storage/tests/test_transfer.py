# pylint: disable=missing-docstring
from pathlib import Path
from time import time
from unittest.mock import MagicMock, patch

from resolwe.storage.connectors import Transfer, connectors
from resolwe.storage.connectors.exceptions import DataTransferError
from resolwe.storage.connectors.transfer import retry_on_transfer_error
from resolwe.test import TestCase


class TransferTest(TestCase):
    def setUp(self):
        self.local = connectors["local"]
        super().setUp()

    def test_ok(self):
        t = Transfer(self.local, self.local)
        with patch.object(Transfer, "transfer_chunk") as transfer_mock:
            t.transfer_objects("base", [{}])
        transfer_mock.assert_called_once_with(Path("base"), [{}])

    def test_max_thread(self):
        t = Transfer(self.local, self.local)
        with patch.object(Transfer, "transfer_chunk") as transfer_mock:
            t.transfer_objects("base", [{}] * 10)
        self.assertEqual(len(transfer_mock.call_args_list), 10)

        with patch.object(Transfer, "transfer_chunk") as transfer_mock:
            t.transfer_objects("base", [{}] * 20)
        self.assertEqual(len(transfer_mock.call_args_list), 10)

        with patch.object(Transfer, "transfer_chunk") as transfer_mock:
            t.transfer_objects("base", [{}] * 20, max_threads=20)
        self.assertEqual(len(transfer_mock.call_args_list), 20)

    def test_exception(self):
        t = Transfer(self.local, self.local)
        with patch.object(Transfer, "transfer_chunk") as transfer_mock:
            transfer_mock.side_effect = [None, DataTransferError]
            with self.assertRaises(DataTransferError):
                t.transfer_objects("test_url", [{}, {}])

    @patch("resolwe.storage.connectors.transfer.ERROR_TIMEOUT", 0.1)
    @patch("resolwe.storage.connectors.transfer.ERROR_MAX_RETRIES", 3)
    def test_retry_transfer(self):
        t = Transfer(self.local, self.local)
        mock: MagicMock = MagicMock(side_effect=[True, True])
        t.transfer = retry_on_transfer_error(mock)
        # with self.assertRaises(DataTransferError):
        t.transfer_objects("test_url", [{"path": "1"}, {"path": "2"}], max_threads=1)
        self.assertEqual(len(mock.call_args_list), 2)

        mock.reset_mock()
        mock.side_effect = [DataTransferError, True, True]
        start = time()
        t.transfer_objects("test_url", [{"path": "1"}, {"path": "2"}], max_threads=1)
        end = time()
        self.assertEqual(len(mock.call_args_list), 3)
        self.assertGreater(end - start, 0.1)

        mock.reset_mock()
        mock.side_effect = [DataTransferError, DataTransferError, True, True]
        start = time()
        t.transfer_objects("test_url", [{"path": "1"}, {"path": "2"}], max_threads=1)
        end = time()
        self.assertEqual(len(mock.call_args_list), 4)
        self.assertGreater(end - start, 2 * 0.1)

        mock.reset_mock()
        mock.side_effect = [DataTransferError, DataTransferError, DataTransferError]
        with self.assertRaises(DataTransferError):
            t.transfer_objects(
                "test_url", [{"path": "1"}, {"path": "2"}], max_threads=1
            )
