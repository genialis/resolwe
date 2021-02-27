"""Compute hashes from stream."""
import hashlib
from io import RawIOBase
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional

import crcmod

if TYPE_CHECKING:
    from os import PathLike


def compute_hashes(file: "PathLike[str]") -> Dict[str, str]:
    """Compute hashes for a given file/directory.

    :param file_: path-like object pointing to a file/directory.

    :returns: dictionary that contains hash types as keys and corresponding
        hashes as values. There is one entry in this dictionary for each hash
        type in StreamHasher.KNOWN_HASH_TYPES.
        If file_ points to a directory values are empty strings.
    """
    path = Path(file)
    if path.is_dir():
        return {hash_type: "" for hash_type in StreamHasher.KNOWN_HASH_TYPES}

    hasher = StreamHasher()
    with path.open("rb") as stream:
        hasher.compute(stream)
    return {
        hash_type: hasher.hexdigest(hash_type)
        for hash_type in StreamHasher.KNOWN_HASH_TYPES
    }


class AWSS3ETagHash:
    """Compute AWSS3etag hash.

    See https://teppen.io/2018/10/23/aws_s3_verify_etags/ for details.
    """

    def __init__(self):
        """Initialize hasher."""
        self.md5_digests = []
        self.hasher = hashlib.md5

    def update(self, data: bytes):
        """Update hasher with given data."""
        self.md5_digests.append(self.hasher(data).digest())

    def hexdigest(self) -> str:
        """Compute and return hexdigest."""
        if len(self.md5_digests) == 1:
            return self.md5_digests[0].hex()
        else:
            joined_md5 = b"".join(self.md5_digests)
            return "{}-{}".format(
                self.hasher(joined_md5).hexdigest(), len(self.md5_digests)
            )


class StreamHasher:
    """Compute hash for data in the stream."""

    KNOWN_HASH_TYPES = ["md5", "crc32c", "awss3etag"]
    _hashers = {
        "awss3etag": AWSS3ETagHash,
        "md5": hashlib.md5,
        "crc32c": lambda: crcmod.predefined.PredefinedCrc("crc32c"),
    }

    def __init__(self, hashes: Optional[List[str]] = None, chunk_size=8 * 1024 * 1024):
        """Initialize the hasher class using given chunk_size.

        Optionally set the hashes we want to compute by setting the hashes
        property. The hashes property must include strings from
        KNOWN_HASH_TYPES list.

        Be careful to set the correct chunk_size for AWSS3ETag computation.
        It must be the same as upload chunk size used to upload file to S3.
        """
        self.chunk_size = chunk_size
        self.hashes = hashes or StreamHasher.KNOWN_HASH_TYPES

    def _init_hashers(self):
        """Initialize known hashers."""
        self._hashers = {name: StreamHasher._hashers[name]() for name in self.hashes}

    def compute(self, stream_in: RawIOBase, stream_out: RawIOBase = None):
        """Compute and return the hash for the given stream.

        The data is read from stream_in until EOF, given to the hasher, and
        written unmodified to the stream_out (unless it is None).

        :param stream_in: input stream.
        :type stream_in: io.RawIOBase

        :param stream_out: output stream.
        :type stream_out: io.RawIOBase
        """
        self._init_hashers()
        read_bytes = self.chunk_size
        while read_bytes == self.chunk_size:
            data = stream_in.read(self.chunk_size)
            read_bytes = len(data)
            for hasher in self._hashers.values():
                hasher.update(data)
            if stream_out is not None:
                stream_out.write(data)

    def digest(self, hash_type: str) -> bytes:
        """Return the digest for the given hash_type.

        :raises KeyError: when hash_type is unknown. Known hash types are
            listed in Hasher.KNOWN_HASH_TYPES.
        :return: computed hash digest of the given type.
        :rtype: bytes
        """
        return bytes.fromhex(self.hexdigest(hash_type))

    def hexdigest(self, hash_type: str) -> str:
        """Return the hex digest for the given hash_type.

        :raises KeyError: when hash_type is unknown. Known hash types are
            listed in Hasher.KNOWN_HASH_TYPES.
        :return: computed hash hexdigest of the given type.
        :rtype: str
        """
        return self._hashers[hash_type].hexdigest().lower()
