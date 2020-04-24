"""Local Storage connector."""
import os
import shutil
from pathlib import Path

from .baseconnector import BaseStorageConnector, validate_url
from .hasher import StreamHasher


class LocalFilesystemConnector(BaseStorageConnector):
    """Local filesystem connector."""

    #: Read files by chunks of the given size
    CHUNK_SIZE = 1024 * 1024

    def __init__(self, config: dict, name: str):
        """Connector initialization."""
        super().__init__(config, name)
        self.path = config["path"]
        self.supported_upload_hash = ["crc32c", "md5"]
        self.supported_download_hash = ["crc32c", "md5"]
        self.multipart_chunksize = self.CHUNK_SIZE

    def delete(self, urls):
        """Remove objects."""
        super().delete(urls)
        for url in urls:
            path = self.base_path / url
            if path.exists():
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()

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

    @validate_url
    def get_object_list(self, url):
        """Get a list of objects stored bellow the given URL."""
        path = self.base_path / url
        return [
            os.fspath((Path(dirpath) / filename).relative_to(self.base_path))
            for dirpath, _, files in os.walk(path)
            for filename in files
            if filename != path
        ]

    @validate_url
    def exists(self, url):
        """Get if the object at the given URL exist."""
        return (self.base_path / url).exists()

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

    def set_hashes(self, url, hashes):
        """Set the  hashes for the given object."""
        pass

    @property
    def base_path(self):
        """Get a base path for this connector."""
        return Path(self.path)

    @validate_url
    def presigned_url(self, url, expiration=3600):
        """Create a presigned URL.

        The URL is used to obtain temporary access to the object ar the
        given URL using only returned URL.

        :param expiration: expiration time of the link (in seconds), default
            is one minute.

        :returns: URL that can be used to access object or None.
        """
        public_url = Path(self.config.get("public_url", "/local_data"))
        resource_url = public_url / url
        return resource_url.as_posix()
