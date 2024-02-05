"""Storage connector."""

import abc
import copy
from enum import Enum, auto
from inspect import getfullargspec
from os import PathLike
from pathlib import PurePath
from typing import BinaryIO, Dict, List, Optional, Union

from wrapt import decorator

DEFAULT_CONNECTOR_PRIORITY = 100


class ConnectorType(Enum):
    """Type of storage connector."""

    LOCAL = auto()
    S3 = auto()
    GCS = auto()
    UNKNOWN = auto()


@decorator
def validate_url(wrapped, instance, args, kwargs):
    """Enforces argument named "url" to be relative path.

    Check that it is instance of str or os.PathLike and that it represents
    relative path.
    """
    try:
        # Use -1 since self is not included in the args.
        url = args[getfullargspec(wrapped).args.index("url") - 1]
    except IndexError:
        url = kwargs.get("url")
    if not isinstance(url, (str, PathLike)):
        raise TypeError("Argument 'url' must be a string or path-like object")
    if PurePath(url).is_absolute():
        raise ValueError("Argument 'url' must be a relative path")
    return wrapped(*args, **kwargs)


@decorator
def validate_urls(wrapped, instance, args, kwargs):
    """Enforces argument named "urls" to be a list of relative paths."""
    try:
        # Use -1 since self is not included in the args.
        urls = args[getfullargspec(wrapped).args.index("urls") - 1]
    except IndexError:
        urls = kwargs.get("urls")
    # Check that URLS is really a list of strings.
    if not isinstance(urls, list):
        raise TypeError("Argument urls must be a list of strings or path-like objects")
    if not all(isinstance(url, (str, PathLike)) for url in urls):
        raise TypeError("Argument urls must be a list of strings or path-like objects")
    # Check that all URLS are relative.
    if any(PurePath(url).is_absolute() for url in urls):
        raise ValueError("Paths must be relative.")
    return wrapped(*args, *kwargs)


class BaseStorageConnector(metaclass=abc.ABCMeta):
    """Base class for storage connectors."""

    REQUIRED_SETTINGS = ["Connector must override REQUIRED_SETTINGS"]
    CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB
    CONNECTOR_TYPE = ConnectorType.UNKNOWN

    def __init__(self, config: dict, name: str):
        """Connector initialization."""
        self.priority = config.get("priority", DEFAULT_CONNECTOR_PRIORITY)
        self.config = copy.deepcopy(config)
        self.name = name
        self.supported_hash = []
        # Some hashes may change after data is transfered, for instance
        # awss3etag will change when SSE-KMS encryption is set on the bucket.
        # These hashes must be refreshed after the transfer is complete.
        self.refresh_hash_after_transfer = []
        # Does connector preserves data integrity during download.
        self.get_ensures_data_integrity = False
        # Does connector preserves data integrity during upload.
        self.put_ensures_data_integrity = False

    @abc.abstractproperty
    def base_path(self) -> PurePath:
        """Get a base path for this connector."""

    def prepare_url(self, url: PathLike, **kwargs):
        """Prepare all the necessary for a new location."""

    @property
    def mountable(self) -> bool:
        """Get a value indicating if connector's data is on a filesystem."""
        return False

    @abc.abstractmethod
    def get_object_list(self, url: Union[str, PathLike]) -> List[str]:
        """Get a list of objects stored bellow the given URL.

        :param url: given URL.
        :type url: str

        :return: a list of paths for objects stored under the given URL. The
            paths are relative with respect to the given URL.
        :rtype: List[str]
        """
        raise NotImplementedError

    def duplicate(self) -> "BaseStorageConnector":
        """Duplicate existing connector.

        Since connector is not thread safe, each thread needs its own instance
        of the connector.
        """
        return self.__class__(self.config, self.name)

    def before_get(self, objects: List[dict], url: Union[str, PathLike]) -> List[dict]:
        """Perform pre-processing before get.

        :param objects: objects to transfer.

        :param url: URL that will be used by transfer.

        :returns: a list of dictionaries with information about files that
            will be actually transfered.
        """
        return objects

    def after_get(self, objects: List[dict], url: Union[str, PathLike]):
        """Perform post-processing after get.

        :param objects: objects that were transfered.

        :param url: URL that was used by transfer.
        """

    def before_push(self, objects: List[dict], url: Union[str, PathLike]):
        """Perform pre-processing before push.

        :param objects: objects to transfer.

        :param url: URL that will be used by transfer.
        """

    def after_push(
        self, objects: List[dict], url: Union[str, PathLike]
    ) -> Optional[List[dict]]:
        """Perform post-processing after push.

        :param objects: objects that were transfered.

        :param url: URL that was used by transfer.

        :return: a list of dictionaries with information about files actually
            stored or None if files were just transfered.
        """
        return objects

    @validate_url
    def check_url(self, url: Union[str, PathLike]) -> bool:
        """Perform check on URL.

        Usually this just checks that object at given url exists, but in some
        cases additional checks are performed.
        """
        return self.exists(url)

    @abc.abstractmethod
    def push(
        self,
        stream: BinaryIO,
        url: Union[str, PathLike],
        chunk_size: int = CHUNK_SIZE,
        hashes: Dict[str, str] = {},
    ):
        """Push data from the stream to the given URL.

        :param stream: given stream.
        :param url: where the data in the stream will be stored.
        :param chunk_size: the chunk_size to use.
        :param hashes: the hashes to set (as metadata) on the uploaded object.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def exists(self, url: Union[str, PathLike]) -> bool:
        """Get if the object at the given URL exist."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_hash(self, url: Union[str, PathLike], hash_type: str) -> Optional[str]:
        """Get the hash of the given type for the given object.

        Hashes are computed using instance of the class
        :class:`~resolwe.storage.connectors.hasher.Hasher` and stored as
        metadata (when supported by the connector).

        :return: the string containg hexdigest for the given object. if given
            object does not exist None is returned.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_hashes(
        self, url: Union[str, PathLike], hash_types: List[str]
    ) -> Optional[Dict[str, str]]:
        """Get the hashes of the given types for the given object."""
        raise NotImplementedError

    @abc.abstractmethod
    def set_hashes(self, url: Union[str, PathLike], hashes: Dict[str, str]):
        """Set the  hashes for the given object.

        :param url: URL of the object.
        :type url: str

        :param hashes: dictionary where key is hash type and value is
            hexdigest of the object for the hash type.
        :type hashes: Dict[str, str]
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get(
        self, url: Union[str, PathLike], stream: BinaryIO, chunk_size: int = CHUNK_SIZE
    ):
        """Get data from the given URL and write it into the given stream.

        :param url: URL of the object.
        :param stream: stream to write data into.
        :param chunk_size: the chunk size to use.
        """
        raise NotImplementedError

    @property
    def can_open_stream(self):
        """Get True if connector can open object as stream."""
        return False

    def open_stream(self, url: Union[str, PathLike], mode: str) -> BinaryIO:
        """Get stream for data at the given URL.

        :param url: URL of the object.

        :param mode: mode in which the stream is opened. See
            https://docs.python.org/3/library/functions.html#open .

        :raises Exception: when stream could not be opened.

        :returns: binary stream if data exists.
        """
        raise NotImplementedError

    def delete(self, url: Union[str, PathLike], urls: List[Union[str, PathLike]]):
        """Remove objects.

        Since delete is potentially harmfull use validate_urls and validate_url
        decorators on the implementation.

        :param url: base URL of the objets to delete.

        :param urls: URLs of the objects to delete. They must be relative with
            respect to url argument.

        :type url: List[str]

        :rtype: None
        """
        raise NotImplementedError

    def multipart_push(
        self,
        upload_id: str,
        url: PathLike,
        part_number: int,
        chunk_size: int,
        data: bytes,
        md5: Optional[str] = None,
    ) -> Dict[str, str]:
        """Upload single part of multipart upload."""
        raise NotImplementedError

    def multipart_push_start(self, url: str, size: Optional[int] = None) -> str:
        """Start a multipart upload."""
        raise NotImplementedError

    def multipart_push_complete(
        self, upload_id: str, url: str, completed_chunks: List
    ) -> str:
        """Complete the multipart push."""
        raise NotImplementedError

    def multipart_push_abort(self, upload_id: str, url: str) -> Dict[str, str]:
        """Abort the multipart push."""
        raise NotImplementedError

    @abc.abstractmethod
    def presigned_url(
        self,
        url: Union[str, PathLike],
        expiration: int = 10,
        force_download: bool = False,
    ) -> Optional[str]:
        """Create a presigned URL.

        The URL is used to obtain temporary access to the object ar the
        given URL using only returned URL.

        :param expiration: expiration time of the link (in seconds), default
            is 10 seconds.

        :param force_download: force download.

        :returns: URL that can be used to access object or None.
        """
        raise NotImplementedError

    @validate_url
    def url(self, url: Union[str, PathLike]) -> Optional[str]:
        """Return the URL to the given file in connector native notation.

        :returns: the url to the given file in connector native notation.
        """
        raise NotImplementedError

    def __eq__(self, other):
        """Equality check.

        Two connectors are considered equal if they have the same name.
        """
        if isinstance(other, BaseStorageConnector):
            return self.name == other.name
        return False

    @classmethod
    def __init_subclass__(cls, **kwargs):
        """Register class with the registry on initialization."""
        super().__init_subclass__(**kwargs)
        from .registry import connectors  # Circular import

        connectors.add_storage_connector_class(cls)

    def __str__(self):
        """Get string representation."""
        return "Connector({})".format(self.name)

    def __repr__(self):
        """Get string representation."""
        return self.__str__()

    def temporary_credentials(self, prefix: str, duration: int = 900, **kwargs) -> Dict:
        """Get the temporary credentials.

        The default implementation returns empty dictionary.
        """
        return {}
