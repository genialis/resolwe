"""Google storage connector."""
import base64
import datetime
import mimetypes
import os
from contextlib import suppress
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import storage

from .baseconnector import BaseStorageConnector, validate_url, validate_urls


class GoogleConnector(BaseStorageConnector):
    """Google Cloud Storage storage connector."""

    REQUIRED_SETTINGS = ["bucket", "credentials"]

    def __init__(self, config: dict, name: str):
        """Initialize Google connector."""
        super().__init__(config, name)
        self.bucket_name = config["bucket"]
        self.supported_hash = ["crc32c", "md5"]
        self.hash_propery = {"md5": "md5_hash", "crc32c": "crc32c"}

    @validate_url
    def get_object_list(self, url):
        """Get a list of objects stored bellow the given URL."""
        url = os.path.join(url, "")
        return [
            Path(e.name).relative_to(url).as_posix()
            for e in self.bucket.list_blobs(prefix=url)
        ]

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

    @validate_urls
    @validate_url
    def delete(self, url, urls):
        """Remove objects."""
        # At most 1000 objects can be deleted at the same time.
        max_chunk_length = 1000
        for i in range(0, len(urls), max_chunk_length):
            with suppress(NotFound):
                next_chunk = urls[i : i + max_chunk_length]
                with self.client.batch():
                    for delete_url in next_chunk:
                        blob = self.bucket.blob(
                            os.fspath(self.base_path / url / delete_url)
                        )
                        blob.delete()

    @validate_url
    def push(self, stream, url, chunk_size=BaseStorageConnector.CHUNK_SIZE):
        """Push data from the stream to the given URL."""
        url = os.fspath(url)
        mime_type = mimetypes.guess_type(url)[0]
        blob = self.bucket.blob(url)
        blob.upload_from_file(stream, content_type=mime_type)

    @validate_url
    def get(self, url, stream, chunk_size=BaseStorageConnector.CHUNK_SIZE):
        """Get data from the given URL and write it into the given stream."""
        blob = self.bucket.blob(os.fspath(url))
        blob.download_to_file(stream)

    @validate_url
    def get_hash(self, url, hash_type):
        """Get the hash of the given type for the given object."""
        blob = self.bucket.get_blob(os.fspath(url))
        if blob is None:
            return None
        blob.update()
        if hash_type in self.hash_propery:
            prop = self.hash_propery[hash_type]
            return base64.b64decode(getattr(blob, prop)).hex()
        else:
            return blob.metadata[hash_type]

    @validate_url
    def get_hashes(self, url, hash_types):
        """Get the hash of the given type for the given object."""
        hashes = dict()
        blob = self.bucket.get_blob(os.fspath(url))
        if blob is None:
            return None
        blob.update()

        for hash_type in hash_types:
            if hash_type in self.hash_propery:
                prop = self.hash_propery[hash_type]
                hashes[hash_type] = base64.b64decode(getattr(blob, prop)).hex()
            else:
                hashes[hash_type] = blob.metadata[hash_type]
        return hashes

    @validate_url
    def set_hashes(self, url, hashes):
        """Set the  hashes for the given object."""
        blob = self.bucket.get_blob(os.fspath(url))
        blob.update()
        meta = blob.metadata or dict()
        hashes = {k: v for (k, v) in hashes.items() if k not in self.hash_propery}
        meta.update(hashes)
        blob.metadata = meta
        blob.update()

    @validate_url
    def exists(self, url):
        """Get if the object at the given URL exist."""
        return storage.Blob(bucket=self.bucket, name=os.fspath(url)).exists()

    @property
    def base_path(self):
        """Get a base path for this connector."""
        return Path("")

    @validate_url
    def presigned_url(self, url, expiration=60, force_download=False):
        """Create a presigned URL.

        The URL is used to obtain temporary access to the object ar the
        given URL using only returned URL.

        :param expiration: expiration time of the link (in seconds), default
            is one minute.

        :param force_download: force download.

        :returns: URL that can be used to access object or None.
        """
        content_disposition = "attachment" if force_download else "inline"
        query_parameters = {"response-content-disposition": content_disposition}
        blob = self.bucket.blob(os.fspath(url))
        response = blob.generate_signed_url(
            version="v4",
            expiration=datetime.timedelta(seconds=expiration),
            method="GET",
            virtual_hosted_style=True,
            query_parameters=query_parameters,
        )
        return response
