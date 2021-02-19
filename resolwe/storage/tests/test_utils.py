# pylint: disable=missing-docstring
import tempfile
from pathlib import Path
from typing import Sequence

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

    def test_chunks(self):
        parts = range(100)
        chunks = list(utils.chunks(parts, 4))
        self.assertEqual(len(chunks), 4)
        for chunk in chunks:
            self.assertEqual(len(chunk), 25)

        chunks = list(utils.chunks(parts, 200))
        self.assertEqual(len(chunks), 200)
        for chunk in chunks[: len(parts)]:
            self.assertEqual(len(chunk), 1)
        for chunk in chunks[len(parts) :]:
            self.assertEqual(len(chunk), 0)

        chunks = list(utils.chunks(parts, 3))
        self.assertEqual(len(chunks), 3)
        self.assertEqual(len(chunks[0]), 34)
        self.assertEqual(len(chunks[1]), 33)
        self.assertEqual(len(chunks[2]), 33)

    def test_paralelize(self):
        parts = [1] * 100

        def worker(parts: Sequence[int]) -> int:
            return sum(parts)

        def worker_exception(parts: Sequence[int]) -> int:
            if len(parts) == 34:
                raise Exception()
            return sum(parts)

        futures = utils.paralelize(parts, worker)
        results = [f.result() for f in futures]
        self.assertEqual(results, [10] * 10)

        futures = utils.paralelize(parts, worker, max_threads=200)
        results = [f.result() for f in futures]
        self.assertEqual(results, [1] * 100)

        futures = utils.paralelize(parts, worker, max_threads=3)
        results = [f.result() for f in futures]
        self.assertEqual(results, [34, 33, 33])

        futures = utils.paralelize(parts, worker_exception, max_threads=3)
        with self.assertRaises(Exception):
            futures[0].result()
        results = [f.result() for f in futures[1:]]
        self.assertEqual(results, [33, 33])
