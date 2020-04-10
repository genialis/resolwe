"""Local Storage connector."""
import os
import shutil

from .baseconnector import BaseStorageConnector
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
        for url in urls:
            path = os.path.join(self.base_path, url)
            if os.path.exists(path):
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)

    def push(self, stream, url):
        """Push data from the stream to the given URL."""
        data_remaining = True
        path = os.path.join(self.base_path, url)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb", self.CHUNK_SIZE) as f:
            while data_remaining:
                data = stream.read(self.CHUNK_SIZE)
                f.write(data)
                data_remaining = len(data) == self.CHUNK_SIZE

    def get(self, url, stream):
        """Get data from the given URL and write it into the given stream."""
        path = os.path.join(self.base_path, url)
        with open(path, "rb", self.CHUNK_SIZE) as f:
            for chunk in iter(lambda: f.read(self.CHUNK_SIZE), b""):
                stream.write(chunk)

    def get_object_list(self, url):
        """Get a list of objects stored bellow the given URL."""
        path = os.path.join(self.base_path, url)
        return [
            os.path.relpath(os.path.join(dirpath, filename), self.base_path)
            for dirpath, _, files in os.walk(path)
            for filename in files
            if filename != path
        ]

    def exists(self, url):
        """Get if the object at the given URL exist."""
        path = os.path.join(self.base_path, url)
        return os.path.exists(path)

    def get_hash(self, url, hash_type):
        """Get the hash of the given type for the given object."""
        hasher = StreamHasher(chunk_size=self.multipart_chunksize)
        path = os.path.join(self.base_path, url)
        if not os.path.exists(path):
            return None
        with open(path, "rb", self.CHUNK_SIZE) as f:
            hasher.compute(f)
        return hasher.hexdigest(hash_type)

    def set_hashes(self, url, hashes):
        """Set the  hashes for the given object."""
        pass

    @property
    def base_path(self):
        """Get a base path for this connector."""
        return self.path
