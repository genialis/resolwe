# pylint: disable=missing-docstring
import hashlib
import os
import tempfile

import crcmod

from resolwe.storage.connectors.hasher import StreamHasher
from resolwe.test import TestCase

tmp_dir = tempfile.TemporaryDirectory()


def path(filename):
    return os.path.join(tmp_dir.name, filename)


def hashes(filename):
    """Compute MD5 and CRC32C hexdigest."""
    hashers = (hashlib.md5(), crcmod.predefined.PredefinedCrc("crc32c"))
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            for hasher in hashers:
                hasher.update(chunk)
    return hashers[0].hexdigest(), hashers[1].hexdigest()


class StreamHasherTest(TestCase):
    def test_hasher(self):
        hasher = StreamHasher()
        with open(path("1"), "wb") as f:
            f.write(b"testingdata" * 10000000)
        with open(path("1"), "rb") as f:
            hasher.compute(f)
        md5_hexdigest, crc32c_hexdigest = hashes(path("1"))
        self.assertEqual(hasher.hexdigest("md5").lower(), md5_hexdigest.lower())
        self.assertEqual(hasher.hexdigest("crc32c").lower(), crc32c_hexdigest.lower())
