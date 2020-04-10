"""Storage connector."""
import abc
from typing import BinaryIO, Dict, List

DEFAULT_CONNECTOR_PRIORITY = 100


class BaseStorageConnector(metaclass=abc.ABCMeta):
    """Base class for storage connectors."""

    def __init__(self, config: dict, name: str):
        """Connector initialization."""
        self.priority = config.get("priority", DEFAULT_CONNECTOR_PRIORITY)
        self.config = config
        self.name = name
        self.supported_upload_hash = []
        self.supported_download_hash = []

    @abc.abstractproperty
    def base_path(self) -> str:
        """Get a base path for this connector."""

    @abc.abstractmethod
    def get_object_list(self, url: str):
        """Get a list of objects stored bellow the given URL.

        :param url: given URL.
        :type url: str

        :return: a list of paths for objects stored under the given URL.
        :rtype: List[str]
        """
        raise NotImplementedError

    @abc.abstractmethod
    def push(self, stream: BinaryIO, url: str):
        """Push data from the stream to the given URL.

        :param stream: given stream.
        :type stream: BinaryIO

        :param url: where the data in the stream will be stored.
        :type url: str
        """
        raise NotImplementedError

    @abc.abstractmethod
    def exists(self, url: str) -> bool:
        """Get if the object at the given URL exist."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_hash(self, url: str, hash_type: str) -> str:
        """Get the hash of the given type for the given object.

        Hashes are computed using instance of the class
        :class:`~resolwe.storage.connectors.hasher.Hasher` and stored as
        metadata (when supported by the connector).

        :return: the string containg hexdigest for the given object.
        :trype: str
        """
        raise NotImplementedError

    @abc.abstractmethod
    def set_hashes(self, url: str, hashes: Dict[str, str]):
        """Set the  hashes for the given object.

        :param url: URL of the object.
        :type url: str

        :param hashes: dictionary where key is hash type and value is
            hexdigest of the object for the hash type.
        :type hashes: Dict[str, str]
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, url: str, stream: BinaryIO):
        """Get data from the given URL and write it into the given stream.

        :param url: URL of the object.
        :type url: str

        :param stream: stream to write data into.
        :type stream: BinaryIO

        :rtype: None
        """
        raise NotImplementedError

    @abc.abstractmethod
    def delete(self, urls: List[str]):
        """Remove objects.

        :param urls: URLs of the objects to delete.
        :type url: List[str]

        :rtype: None
        """
        raise NotImplementedError

    @classmethod
    def __init_subclass__(cls, **kwargs):
        """Register class with the registry on initialization."""
        super().__init_subclass__(**kwargs)
        from .registry import StorageConnectors  # Circular import

        StorageConnectors().add_storage_connector_class(cls)

    def __str__(self):
        """Get string representation."""
        return "Connector({})".format(self.name)

    def __repr__(self):
        """Get string representation."""
        return self.__str__()
