# pylint: disable=missing-docstring
import shutil
from pathlib import Path
from time import time
from unittest.mock import MagicMock, patch

from resolwe.storage.connectors import (
    LocalFilesystemConnector,
    Transfer,
    connectors,
)
from resolwe.storage.connectors.exceptions import DataTransferError
from resolwe.storage.connectors.transfer import retry_on_transfer_error
from resolwe.storage.connectors.utils import get_transfer_object
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

    @patch("resolwe.storage.connectors.transfer.ERROR_INITIAL_TIMEOUT", 0.1)
    @patch("resolwe.storage.connectors.transfer.ERROR_MAX_TIMEOUT", 0.1)
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

    def test_abort_no_retry(self):
        t = Transfer(self.local, self.local)
        calls = []

        @retry_on_transfer_error
        def transfer(self, *args, **kwargs):
            calls.append(args)
            raise DataTransferError()

        with patch.object(Transfer, "transfer", transfer):
            t.abort()
            with self.assertRaises(DataTransferError):
                t.transfer("base", {}, "base", "1")
        self.assertEqual(len(calls), 1)

    def test_abort_stops_before_next_object(self):
        t = Transfer(self.local, self.local)

        def transfer(*args, **kwargs):
            t.abort()
            return True

        with patch.object(Transfer, "transfer", side_effect=transfer) as transfer_mock:
            with self.assertRaisesRegex(DataTransferError, "aborted"):
                t.transfer_chunk(Path("base"), [{"path": "1"}, {"path": "2"}])
        transfer_mock.assert_called_once()

    def test_abort_closes_stream(self):
        base = self.local.base_path / "abort_test"
        base.mkdir(parents=True, exist_ok=True)
        self.addCleanup(shutil.rmtree, base, ignore_errors=True)
        source = base / "source.txt"
        source.write_bytes(b"data")
        entry = get_transfer_object(source, base)
        entry.update({"from_base_url": "abort_test", "to_base_url": "abort_copy"})
        destination = self.local.base_path / "abort_copy" / "source.txt"
        t = Transfer(self.local, self.local)

        def push(connector, stream, url, chunk_size=None, hashes=None):
            self.assertFalse(stream.closed)
            t.abort()
            self.assertTrue(stream.closed)
            # A real push fails reading the closed stream.
            stream.read(1)

        with patch.object(LocalFilesystemConnector, "push", push):
            with self.assertRaisesRegex(DataTransferError, "aborted"):
                t.transfer_chunk(Path(""), [entry])
        self.assertFalse(destination.exists())

    def test_abort_stops_buffered_download(self):
        """The download into the buffer fails on its next write after abort."""

        class Connector:
            can_open_stream = False
            get_ensures_data_integrity = False
            put_ensures_data_integrity = False
            supported_hash = ["md5"]
            refresh_hash_after_transfer = []
            name = "buffered"

            def duplicate(self):
                return self

            def get_hash(self, url, hash_type):
                return None

            def delete(self, url, urls):
                pass

        written = []

        def get(url, stream, chunk_size):
            # Like the S3 and GCS connectors, ignore the return of write.
            for chunk in range(10):
                stream.write(b"x")
                written.append(chunk)
                if chunk == 0:
                    t.abort()

        def push(stream, url, chunk_size, hashes):
            while stream.read(1):
                pass

        source, destination = Connector(), Connector()
        source.get, destination.push = get, push
        t = Transfer(source, destination)
        entry = {"path": "1", "size": 10, "md5": "a", "crc32c": "a", "awss3etag": "a"}
        with self.assertRaisesRegex(DataTransferError, "aborted"):
            t.transfer_chunk(Path("base"), [entry])
        self.assertEqual(written, [0])
