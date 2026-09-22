# pylint: disable=missing-docstring
import asyncio
import concurrent.futures
import tempfile
import threading
from pathlib import Path
from time import time
from typing import Iterable, Sequence

from resolwe.storage.connectors import utils
from resolwe.test import TestCase


def create_file(file: Path, size: int):
    with file.open("wb") as f:
        f.seek(size - 1)
        f.write(b"\0")
        f.seek(0)
        f.write(file.name.encode("utf-8"))


class UtilsTest(TestCase):
    def test_get_transfer_object_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            file = tmp_path / "file.in"
            create_file(file, 1000)
            to_transfer = utils.get_transfer_object(file, tmp_path)
            self.assertDictEqual(
                to_transfer,
                {
                    "size": 1000,
                    "path": "file.in",
                    "md5": "9638a76565e9f1f15443421c5605d8ce",
                    "crc32c": "5e29c10c",
                    "awss3etag": "9638a76565e9f1f15443421c5605d8ce",
                    "chunk_size": 8 * 1024 * 1024,
                },
            )

    def test_get_transfer_object_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            directory: Path = tmp_path / "dir"
            directory.mkdir()
            to_transfer = utils.get_transfer_object(directory, tmp_path)
            self.assertDictEqual(
                to_transfer,
                {
                    "size": 0,
                    "path": "dir/",
                    "md5": "",
                    "crc32c": "",
                    "awss3etag": "",
                },
            )

    def test_get_transfer_object_nonexisting(self):
        to_transfer = utils.get_transfer_object("base/nonexisting.txt", "base")
        self.assertIsNone(to_transfer)

    def test_shared_iterator(self):
        shared = utils.SharedIterator(range(1000))
        with concurrent.futures.ThreadPoolExecutor(4) as executor:
            consumed = list(executor.map(lambda _: list(shared), range(4)))
        self.assertEqual(sorted(sum(consumed, [])), list(range(1000)))

    def test_largest_first(self):
        objects = [{"size": 1}, {"path": "dir/"}, {"size": 3}, {"size": 2}]
        self.assertEqual(
            utils.largest_first(objects),
            [{"size": 3}, {"size": 2}, {"size": 1}, {"path": "dir/"}],
        )

    def test_paralelize(self):
        parts = [1] * 100

        def worker(parts: Iterable[int]) -> int:
            return sum(parts)

        def worker_exception(parts: Iterable[int]) -> int:
            for part in parts:
                if part == 2:
                    raise Exception()
            return 0

        results = utils.paralelize(parts, worker)
        self.assertEqual(len(results), 10)
        self.assertEqual(sum(results), 100)

        results = utils.paralelize(parts, worker, max_threads=200)
        self.assertEqual(len(results), 100)
        self.assertEqual(sum(results), 100)

        self.assertEqual(utils.paralelize([], worker), [])

        with self.assertRaises(Exception):
            utils.paralelize([1, 2, 1], worker_exception, max_threads=1)

    def test_paralelize_abort(self):
        aborted = threading.Event()

        def worker(parts: Iterable[str]) -> bool:
            for part in parts:
                if part == "bad":
                    raise ValueError("bad")
                # Runs until aborted, then fails like an aborted transfer.
                if aborted.wait(5):
                    raise RuntimeError("aborted")
            return True

        start = time()
        with self.assertRaisesRegex(ValueError, "bad"):
            utils.paralelize(["bad", "good"], worker, max_threads=2, abort=aborted.set)
        self.assertTrue(aborted.is_set())
        self.assertLess(time() - start, 2)

    def test_paralelize_shares_work(self):
        fast_done = threading.Event()

        def worker(parts: Iterable[str]) -> list:
            done = []
            for part in parts:
                if part == "slow":
                    fast_done.wait()
                done.append(part)
            # The worker that drains the queue releases the slow one.
            fast_done.set()
            return done

        threading.Timer(5, fast_done.set).start()
        results = utils.paralelize(["slow"] + ["fast"] * 9, worker, max_threads=2)
        self.assertEqual(sorted(len(done) for done in results), [1, 9])

    def test_async_paralelize_first_exception(self):
        release = threading.Event()

        def worker(parts: Iterable[int]) -> int:
            for part in parts:
                if part == 1:
                    raise ValueError()
                release.wait()
            return 0

        async def first_exception():
            loop = asyncio.get_running_loop()
            tasks = await utils.async_paralelize(
                [1, 2], worker, loop, max_threads=2, return_when=asyncio.FIRST_EXCEPTION
            )
            pending = [task for task in tasks if not task.done()]
            release.set()
            await asyncio.wait(tasks)
            return pending

        threading.Timer(2, release.set).start()
        try:
            pending = asyncio.run(first_exception())
        finally:
            release.set()
        self.assertEqual(len(pending), 1)

    def test_async_paralelize_cancel(self):
        started = threading.Event()
        release = threading.Event()

        def worker(parts: Sequence[int]) -> int:
            started.set()
            release.wait()
            return sum(parts)

        async def cancel_running():
            loop = asyncio.get_running_loop()
            task = asyncio.ensure_future(
                utils.async_paralelize([1, 2], worker, loop, max_threads=1)
            )
            await asyncio.to_thread(started.wait)
            task.cancel()
            start = time()
            with self.assertRaises(asyncio.CancelledError):
                await task
            return time() - start

        # Release the worker later so a cancel blocked on it fails, not hangs.
        threading.Timer(2, release.set).start()
        try:
            elapsed = asyncio.run(cancel_running())
        finally:
            release.set()
        self.assertLess(elapsed, 1)
