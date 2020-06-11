"""Local Storage connector."""
import os
import shutil
from pathlib import Path

from .baseconnector import BaseStorageConnector, validate_url, validate_urls
from .hasher import StreamHasher


class LocalFilesystemConnector(BaseStorageConnector):
    """Local filesystem connector."""

    #: Read files by chunks of the given size
    REQUIRED_SETTINGS = ["path"]

    def __init__(self, config: dict, name: str):
        """Connector initialization."""
        super().__init__(config, name)
        self.path = config["path"]
        self.supported_hash = ["crc32c", "md5", "awss3etag"]
        self.multipart_chunksize = self.CHUNK_SIZE

    @validate_urls
    @validate_url
    def delete(self, url, urls):
        """Remove objects."""
        for delete_url in urls:
            path = self.base_path / url / delete_url
            if path.exists():
                if path.is_dir():
                    shutil.rmtree(os.fspath(path))
                else:
                    path.unlink()
        # Remove url base directory if empty.
        if not self.get_object_list(url):
            if (self.base_path / url).is_dir():
                shutil.rmtree(os.fspath(self.base_path / url))

    @validate_url
    def push(self, stream, url):
        """Push data from the stream to the given URL."""
        data_remaining = True
        path = self.base_path / url
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb", self.CHUNK_SIZE) as f:
            while data_remaining:
                data = stream.read(self.CHUNK_SIZE)
                f.write(data)
                data_remaining = len(data) == self.CHUNK_SIZE

    @validate_url
    def get(self, url, stream):
        """Get data from the given URL and write it into the given stream."""
        path = self.base_path / url
        with path.open("rb", self.CHUNK_SIZE) as f:
            for chunk in iter(lambda: f.read(self.CHUNK_SIZE), b""):
                stream.write(chunk)

    @property
    def can_open_stream(self):
        """Get True if connector can open object as stream."""
        return True

    @validate_url
    def open_stream(self, url, mode):
        """Get stream for data at the given URL."""
        path: Path = self.base_path / url
        path.parent.mkdir(parents=True, exist_ok=True)
        return path.open(mode, self.CHUNK_SIZE)

    @validate_url
    def get_object_list(self, url):
        """Get a list of objects stored bellow the given URL."""
        path = self.base_path / url
        return [
            os.fspath((Path(dirpath) / filename).relative_to(path))
            for dirpath, _, files in os.walk(path)
            for filename in files
            if filename != path
        ]

    @validate_url
    def exists(self, url):
        """Get if the object at the given URL exist."""
        return (self.base_path / url).exists()

    @validate_url
    def get_hashes(self, url, hash_types):
        """Get the hash of the given type for the given object."""
        hasher = StreamHasher(chunk_size=self.multipart_chunksize)
        path = self.base_path / url
        if not path.exists():
            return None
        with path.open("rb", self.CHUNK_SIZE) as f:
            hasher.compute(f)
        return {hash_type: hasher.hexdigest(hash_type) for hash_type in hash_types}

    @validate_url
    def get_hash(self, url, hash_type):
        """Get the hash of the given type for the given object."""
        hasher = StreamHasher(chunk_size=self.multipart_chunksize)
        path = self.base_path / url
        if not path.exists():
            return None
        with path.open("rb", self.CHUNK_SIZE) as f:
            hasher.compute(f)
        return hasher.hexdigest(hash_type)

    @validate_url
    def set_hashes(self, url, hashes):
        """Set the  hashes for the given object.

        Local connector has currently no way to store metadata alongside files
        so this call is silently ignored. Hash is calculated always when
        get_hash(es) method is called.
        """

    @property
    def base_path(self):
        """Get a base path for this connector."""
        return Path(self.path)

    @validate_url
    def presigned_url(self, url, expiration=3600, force_download=False):
        """Create a presigned URL.

        The URL is used to obtain temporary access to the object ar the
        given URL using only returned URL.

        :param expiration: expiration time of the link (in seconds), default
            is one minute.

        :param force_download: force download.

        :returns: URL that can be used to access object or None.
        """
        force_download = "?force_download=1" if force_download else ""
        public_url = Path(self.config.get("public_url", "/local_data"))
        resource_url = public_url / url
        return resource_url.as_posix() + force_download
