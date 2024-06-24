"""Thread-safe singleton class."""

from os import getpid
from threading import Lock


class Singleton:
    """Class containing the callback for zmq authentication."""

    _instance = None
    _instance_lock = Lock()
    _instance_pid: int | None = None

    @classmethod
    def has_instance(cls):
        """Check if the instance exists."""
        return not (cls._instance is None or cls._instance_pid != getpid())

    @classmethod
    def instance(cls, *args):
        """Return a global instance of the class."""
        if not cls.has_instance():
            with cls._instance_lock:
                if not cls.has_instance():
                    cls._instance = cls(*args)
                    cls._instance_pid = getpid()
        return cls._instance
