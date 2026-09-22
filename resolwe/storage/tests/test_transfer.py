# pylint: disable=missing-docstring
import shutil
import threading
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

        @retry_on_transfer_error
        def transfer(self, *args, **kwargs):
            return mock(*args, **kwargs)

        # Patch the class so the decorator gets the Transfer instance.
        patcher = patch.object(Transfer, "transfer", transfer)
        patcher.start()
        self.addCleanup(patcher.stop)
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

    @patch("resolwe.storage.connectors.transfer.ERROR_INITIAL_TIMEOUT", 10)
    def test_retry_aborted_while_waiting(self):
        t = Transfer(self.local, self.local)
        calls = []

        @retry_on_transfer_error
        def transfer(self, *args, **kwargs):
            calls.append(args)
            threading.Timer(0.1, t.abort).start()
            raise DataTransferError()

        with patch.object(Transfer, "transfer", transfer):
            start = time()
            with self.assertRaises(DataTransferError):
                t.transfer("base", {}, "base", "1")
        self.assertLess(time() - start, 5)
        self.assertEqual(len(calls), 1)

    def test_failure_names_object(self):
        t = Transfer(self.local, self.local)
        with patch.object(Transfer, "transfer", side_effect=ValueError("boom")):
            with self.assertRaises(DataTransferError) as context:
                t.transfer_chunk(Path("base"), [{"path": "1"}])
        self.assertIn("base/1", str(context.exception))
        self.assertIn("boom", str(context.exception))
        self.assertIsInstance(context.exception.__cause__, ValueError)

        with patch.object(Transfer, "transfer", return_value=False):
            with self.assertRaisesRegex(DataTransferError, "base/1"):
                t.transfer_chunk(Path("base"), [{"path": "1"}])

        # A DataTransferError already names the object and is not wrapped.
        error = DataTransferError("Hash md5 does not match while transfering base/1")
        with patch.object(Transfer, "transfer", side_effect=error):
            with self.assertRaises(DataTransferError) as context:
                t.transfer_chunk(Path("base"), [{"path": "1"}])
        self.assertIs(context.exception, error)

    @patch("resolwe.storage.connectors.transfer.ERROR_MAX_RETRIES", 1)
    def test_partial_object_removed_on_failure(self):
        class FailingConnector:
            """Connector without streams whose get writes a part and fails."""

            name = "failing"
            can_open_stream = False
            get_ensures_data_integrity = False
            supported_hash = ["md5"]

            def duplicate(self):
                return self

            def get(self, url, stream, chunk_size=None):
                stream.write(b"partial")
                raise ConnectionResetError()

        destination = self.local.base_path / "retry_copy" / "1"
        self.addCleanup(shutil.rmtree, destination.parent, ignore_errors=True)
        entry = {"path": "1", "size": 7, "md5": "a", "crc32c": "b", "awss3etag": "c"}
        t = Transfer(FailingConnector(), self.local)
        with self.assertRaises(DataTransferError):
            t.transfer_chunk(Path("retry_copy"), [entry])
        self.assertFalse(destination.exists())

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

    def test_transfer_objects_stops_on_failure(self):
        t = Transfer(self.local, self.local)

        def transfer_chunk(url, objects):
            if objects[0]["path"] == "bad":
                raise DataTransferError("bad")
            # The other worker stops only when the transfer is aborted.
            if not t.wait_aborted(5):
                return True
            raise DataTransferError("Transfer aborted.")

        start = time()
        with patch.object(t, "transfer_chunk", side_effect=transfer_chunk):
            with self.assertRaisesRegex(DataTransferError, "bad"):
                t.transfer_objects(
                    "base", [{"path": "bad"}, {"path": "good"}], max_threads=2
                )
        self.assertTrue(t.aborted)
        self.assertLess(time() - start, 2)

    def test_retry_client_error(self):
        try:
            from botocore.exceptions import ClientError
        except ModuleNotFoundError:
            self.skipTest("botocore is not installed")

        def client_error(code, status):
            return ClientError(
                {
                    "Error": {"Code": code},
                    "ResponseMetadata": {"HTTPStatusCode": status},
                },
                "GetObject",
            )

        class FakeTransfer:
            aborted = False

            def __init__(self, error):
                self.error = error
                self.calls = 0

            def wait_aborted(self, timeout):
                return False

            @retry_on_transfer_error
            def transfer(self):
                self.calls += 1
                raise self.error

        cases = [
            ("NoSuchKey", 404, 1),
            ("AccessDenied", 403, 1),
            ("InvalidObjectState", 403, 1),
            ("SlowDown", 503, 3),
            ("InternalError", 500, 3),
            ("TooManyRequests", 429, 3),
            ("RequestTimeout", 400, 3),
        ]
        with patch("resolwe.storage.connectors.transfer.ERROR_MAX_RETRIES", 3):
            for code, status, attempts in cases:
                fake = FakeTransfer(client_error(code, status))
                with self.assertRaises(ClientError):
                    fake.transfer()
                self.assertEqual(fake.calls, attempts, code)

    def test_retry_s3_download_read_error(self):
        try:
            from s3transfer.exceptions import RetriesExceededError
        except ModuleNotFoundError:
            self.skipTest("s3transfer is not installed")
        from botocore.exceptions import ReadTimeoutError

        class FakeTransfer:
            aborted = False
            calls = 0

            def wait_aborted(self, timeout):
                return False

            @retry_on_transfer_error
            def transfer(self):
                self.calls += 1
                # What download_fileobj raises when every read of the body fails.
                raise RetriesExceededError(ReadTimeoutError(endpoint_url="url"))

        fake = FakeTransfer()
        with patch("resolwe.storage.connectors.transfer.ERROR_MAX_RETRIES", 3):
            with self.assertRaises(RetriesExceededError):
                fake.transfer()
        self.assertEqual(fake.calls, 3)

    @patch("resolwe.storage.connectors.transfer.ERROR_MAX_RETRIES", 1)
    def test_buffered_failure_names_object(self):
        class Connector:
            """Connector without streams, so the transfer uses the buffer."""

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

            def get(self, url, stream, chunk_size):
                raise ConnectionResetError()

            def push(self, stream, url, chunk_size, hashes):
                while stream.read(1):
                    pass

        entry = {"path": "1", "size": 1, "md5": "a", "crc32c": "a", "awss3etag": "a"}
        t = Transfer(Connector(), Connector())
        with self.assertRaises(DataTransferError) as context:
            t.transfer_chunk(Path("base"), [entry])
        self.assertEqual(
            str(context.exception),
            "Transfer of 'base/1' failed: ConnectionResetError()",
        )

    @patch("resolwe.storage.connectors.transfer.ERROR_INITIAL_TIMEOUT", 0.01)
    @patch("resolwe.storage.connectors.transfer.ERROR_MAX_RETRIES", 3)
    def test_buffered_retry_client_error(self):
        """A buffered transfer retries a client error only when it is transient."""
        try:
            from botocore.exceptions import ClientError
        except ModuleNotFoundError:
            self.skipTest("botocore is not installed")

        class Connector:
            """Connector without streams, so the transfer uses the buffer."""

            can_open_stream = False
            get_ensures_data_integrity = False
            put_ensures_data_integrity = False
            supported_hash = ["md5"]
            refresh_hash_after_transfer = []
            name = "buffered"

            def __init__(self, error=None):
                self.error = error
                self.calls = 0

            def duplicate(self):
                return self

            def get_hash(self, url, hash_type):
                return None

            def delete(self, url, urls):
                pass

            def get(self, url, stream, chunk_size):
                self.calls += 1
                raise self.error

            def push(self, stream, url, chunk_size, hashes):
                while stream.read(1):
                    pass

        entry = {"path": "1", "size": 1, "md5": "a", "crc32c": "a", "awss3etag": "a"}
        for code, status, attempts in [("NoSuchKey", 404, 1), ("SlowDown", 503, 3)]:
            error = ClientError(
                {
                    "Error": {"Code": code},
                    "ResponseMetadata": {"HTTPStatusCode": status},
                },
                "GetObject",
            )
            source = Connector(error)
            t = Transfer(source, Connector())
            with self.assertRaises(DataTransferError) as context:
                t.transfer_chunk(Path("base"), [entry])
            self.assertIs(context.exception.__cause__, error)
            self.assertEqual(source.calls, attempts, code)

    @patch("resolwe.storage.connectors.transfer.ERROR_MAX_RETRIES", 1)
    def test_partial_object_removed_on_failed_stream(self):
        """A failed push from a stream leaves no partial object behind."""

        class Stream:
            """Return one chunk, then fail like a dropped connection."""

            def __init__(self):
                self.reads = 0
                self.closed = False

            def read(self, size=-1):
                self.reads += 1
                if self.reads == 1:
                    return b"x" * size
                raise ConnectionResetError()

            def close(self):
                self.closed = True

        class StreamConnector:
            """Source connector that opens streams, like the result upload."""

            name = "stream"
            can_open_stream = True
            get_ensures_data_integrity = False
            supported_hash = ["md5"]

            def duplicate(self):
                return self

            def open_stream(self, url, mode):
                return Stream()

        destination = self.local.base_path / "stream_copy" / "1"
        self.addCleanup(shutil.rmtree, destination.parent, ignore_errors=True)
        entry = {
            "path": "1",
            "size": 8,
            "chunk_size": 4,
            "md5": "a",
            "crc32c": "b",
            "awss3etag": "c",
        }
        t = Transfer(StreamConnector(), self.local)
        with self.assertRaisesRegex(DataTransferError, "ConnectionResetError"):
            t.transfer_chunk(Path("stream_copy"), [entry])
        self.assertFalse(destination.exists())
