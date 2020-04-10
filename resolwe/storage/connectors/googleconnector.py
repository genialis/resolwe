"""Google storage connector."""
import base64
import os
from contextlib import suppress

from google.api_core.exceptions import NotFound
from google.cloud import storage

from .baseconnector import BaseStorageConnector


class GoogleConnector(BaseStorageConnector):
    """Google Cloud Storage storage connector."""

    def __init__(self, config: dict, name: str):
        """Initialize Google connector."""
        super().__init__(config, name)
        self.bucket_name = config["bucket"]
        self.supported_upload_hash = ["crc32c", "md5"]
        self.supported_download_hash = ["crc32c", "md5", "awss3etag"]
        self.hash_propery = {"md5": "md5_hash", "crc32c": "crc32c"}

    def get_object_list(self, url):
        """Get a list of objects stored bellow the given URL."""
        url = os.path.join(url, "")
        return [e.name for e in self.bucket.list_blobs(prefix=url)]

    def _initialize(self):
        """Perform initialization."""
        credentials = self.config["credentials"]
        self.client = storage.Client.from_service_account_json(credentials)
        self.bucket = self.client.get_bucket(self.bucket_name)

    def __getattr__(self, name):
        """Lazy initialize some attributes."""
        requires_initialization = ["client", "bucket"]
        if name not in requires_initialization:
            raise AttributeError()

        self._initialize()
        return getattr(self, name)

    def delete(self, urls):
        """Remove objects."""
        with suppress(NotFound):
            with self.client.batch():
                for to_delete in urls:
                    blob = self.bucket.blob(to_delete)
                    if blob.exists():
                        blob.delete()

    def push(self, stream, url, hash_type=None, data_hash=None):
        """Push data from the stream to the given URL."""

        blob = self.bucket.blob(url)
        if hash_type is not None:
            assert hash_type in self.supported_upload_hash
            prop = self.hash_propery[hash_type]
            setattr(blob, prop, data_hash)
        blob.upload_from_file(stream)

    def get(self, url, stream):
        """Get data from the given URL and write it into the given stream."""
        blob = self.bucket.blob(url)
        blob.download_to_file(stream)

    def get_hash(self, url, hash_type):
        """Get the hash of the given type for the given object."""
        assert hash_type in self.supported_download_hash
        blob = self.bucket.get_blob(url)
        if blob is None:
            return None
        blob.update()
        if hash_type in self.hash_propery:
            prop = self.hash_propery[hash_type]
            return base64.b64decode(getattr(blob, prop)).hex()
        else:
            return blob.metadata[hash_type]

    def set_hashes(self, url, hashes):
        """Set the  hashes for the given object."""
        blob = self.bucket.get_blob(url)
        blob.update()
        meta = blob.metadata or dict()
        hashes = {k: v for (k, v) in hashes.items() if k not in self.hash_propery}
        meta.update(hashes)
        blob.metadata = meta
        blob.update()

    def exists(self, url):
        """Get if the object at the given URL exist."""
        return storage.Blob(bucket=self.bucket, name=url).exists()

    @property
    def base_path(self):
        """Get a base path for this connector."""
        return ""
