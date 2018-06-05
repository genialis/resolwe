"""Abstract Workflow Engine."""
import os
import pkgutil
from importlib import import_module

from django.core.exceptions import ImproperlyConfigured
from django.utils._os import upath


class InvalidEngineError(Exception):
    """Raised when an invalid engine is requested."""

    pass


class BaseEngine:
    """Base class for all engines in Resolwe workflow."""

    name = None

    def __init__(self, manager, settings=None):
        """Construct a Resolwe engine."""
        self.manager = manager
        self.settings = settings or {}

    def get_name(self):
        """Return the engine name."""
        return self.name


def load_engines(manager, class_name, base_module, engines, class_key='ENGINE', engine_type='engine'):
    """Load engines."""
    loaded_engines = {}

    for module_name_or_dict in engines:
        if not isinstance(module_name_or_dict, dict):
            module_name_or_dict = {
                class_key: module_name_or_dict
            }

        try:
            module_name = module_name_or_dict[class_key]
            engine_settings = module_name_or_dict
        except KeyError:
            raise ImproperlyConfigured("If {} specification is a dictionary, it must define {}".format(
                engine_type, class_key))

        try:
            engine_module = import_module(module_name)
            try:
                engine = getattr(engine_module, class_name)(manager=manager, settings=engine_settings)
                if not isinstance(engine, BaseEngine):
                    raise ImproperlyConfigured("{} module {} class {} must extend BaseEngine".format(
                        engine_type.capitalize(), module_name, class_name))
            except AttributeError:
                raise ImproperlyConfigured("{} module {} is missing a {} class".format(
                    engine_type.capitalize(), module_name, class_name))

            if engine.get_name() in loaded_engines:
                raise ImproperlyConfigured("Duplicated {} {}".format(engine_type, engine.get_name()))

            loaded_engines[engine.get_name()] = engine

        except ImportError as ex:
            # The engine wasn't found. Display a helpful error message listing all possible
            # (built-in) engines.
            engine_dir = os.path.join(os.path.dirname(upath(__file__)), base_module)

            try:
                builtin_engines = [name for _, name, _ in pkgutil.iter_modules([engine_dir])]
            except EnvironmentError:
                builtin_engines = []

            if module_name not in ['resolwe.flow.{}.{}'.format(base_module, builtin_engine)
                                   for builtin_engine in builtin_engines]:
                engine_reprs = map(repr, sorted(builtin_engines))
                error_msg = ("{} isn't an available dataflow {}.\n"
                             "Try using 'resolwe.flow.{}.XXX', where XXX is one of:\n"
                             "    {}\n"
                             "Error was: {}".format(
                                 module_name, engine_type, base_module, ", ".join(engine_reprs), ex
                             ))
                raise ImproperlyConfigured(error_msg)
            else:
                # If there's some other error, this must be an error in Django
                raise

    return loaded_engines
