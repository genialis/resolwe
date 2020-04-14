"""Support for easier dynamic composition of type extensions."""
import inspect
from importlib import import_module

from django.apps import apps


class Composer:
    """Support for easier dynamic composition of type extensions."""

    def __init__(self):
        """Construct new composer instance."""
        self._discovery_done = False
        self._extensions = {}

    def discover_extensions(self):
        """Discover available extensions."""
        if self._discovery_done:
            return

        try:
            previous_state = self._extensions.copy()

            for app_config in apps.get_app_configs():
                indexes_path = "{}.extensions".format(app_config.name)
                try:
                    import_module(indexes_path)
                except ImportError:
                    pass

            self._discovery_done = True
        except Exception:
            # Rollback state to prevent corrupted state on exceptions during import.
            self._extensions = previous_state
            raise

    def _get_class_path(self, klass_or_instance):
        """Return class path for a given class.

        :param klass_or_instance: Class or instance of given class or
            class path as a string
        :return: String containing the class path
        """

        def construct_class_path(klass):
            """Return full path of the class."""
            return "{}.{}".format(klass.__module__, klass.__name__)

        if inspect.isclass(klass_or_instance):
            klass = construct_class_path(klass_or_instance)
        elif not isinstance(klass_or_instance, str):
            klass = construct_class_path(klass_or_instance.__class__)
        else:
            klass = klass_or_instance

        return klass

    def add_extension(self, klass, extension):
        """Register an extension for a class.

        :param klass: Class to register an extension for
        :param extension: Extension (arbitrary type)
        """
        klass = self._get_class_path(klass)

        # TODO: Take order into account.
        self._extensions.setdefault(klass, []).append(extension)

    def get_extensions(self, klass):
        """Return all registered extensions of a class.

        :param klass: Class to get registered extensions for
        :return: All registered extensions for given class
        """
        self.discover_extensions()

        return self._extensions.get(self._get_class_path(klass), [])


composer = Composer()
