# pylint: disable=missing-docstring
from pathlib import Path
from unittest.mock import patch

from resolwe.storage.connectors import Transfer, connectors
from resolwe.storage.connectors.exceptions import DataTransferError
from resolwe.test import TestCase


class TransferTest(TestCase):
    def setUp(self):
        self.local = connectors["local"]
        super().setUp()

    def test_ok(self):
        t = Transfer(self.local, self.local)
        with patch.object(Transfer, "_transfer_chunk") as transfer_mock:
            t.transfer_objects("base", [{}])
        transfer_mock.assert_called_once_with(Path("base"), [{}])

    def test_max_thread(self):
        t = Transfer(self.local, self.local)
        with patch.object(Transfer, "_transfer_chunk") as transfer_mock:
            t.transfer_objects("base", [{}] * 10)
        self.assertEqual(len(transfer_mock.call_args_list), 10)

        with patch.object(Transfer, "_transfer_chunk") as transfer_mock:
            t.transfer_objects("base", [{}] * 20)
        self.assertEqual(len(transfer_mock.call_args_list), 10)

        with patch.object(Transfer, "_transfer_chunk") as transfer_mock:
            t.transfer_objects("base", [{}] * 20, max_threads=20)
        self.assertEqual(len(transfer_mock.call_args_list), 20)

    def test_exception(self):
        t = Transfer(self.local, self.local)
        with patch.object(Transfer, "_transfer_chunk") as transfer_mock:
            transfer_mock.side_effect = [None, DataTransferError]
            with self.assertRaises(DataTransferError):
                t.transfer_objects("test_url", [{}, {}])
