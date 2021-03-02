"""Runtimes manager."""
import logging
from contextlib import suppress
from importlib import import_module
from types import ModuleType
from typing import Dict, Optional, Set, Type

from .runtime import Process

logger = logging.getLogger(__name__)


class RegisteredRuntimes:
    """A list of registered base python processes to be copied into executor.

    The python processes is must extend the base class for
    python processes :class:`resolwe.process.runtime.Process` and set the
    class attribute `_abstract` to True. They must also be registered by
    addind their name to the settings `FLOW_PROCESSES_RUNTIMES`.

    Is such case the entire parent module of the file in which the process is
    defined is copied to the executor. It is the responsibility of the
    programer to make sure that the module uses only Python standard library
    (and possibly other runtimes it inherits from).
    """

    def __init__(self):
        """Initialize."""
        self._class_module: Dict[str, ModuleType] = dict()
        self._registered_classes: Dict[str, Type[Process]] = dict()
        # A set of dependencies for every registered class.
        self._dependencies: Dict[str, Set[ModuleType]] = dict()
        self._register_runtimes()

    def _register_runtimes(self):
        """Register python runtimes."""
        with suppress(ImportError):
            from django.conf import settings

            for runtime_class_name in getattr(
                settings,
                "FLOW_PROCESSES_RUNTIMES",
                ("resolwe.process.runtime.Process",),
            ):
                module_name, class_name = runtime_class_name.rsplit(".", 1)
                self.register_runtime(getattr(import_module(module_name), class_name))

    def _get_runtime_classes(self, klass: Type["Process"]) -> Set[Type["Process"]]:
        """Get all the runtime classes of a given class.

        This includes all the runtime classes this class inherits from.
        """
        bases = set()
        if "_abstract" in klass.__dict__ and issubclass(klass, Process):
            bases.add(klass)
        for base_class in klass.__bases__:
            bases.update(self._get_runtime_classes(base_class))
        return bases

    def register_runtime(self, klass: Type["Process"]):
        """Register base class for processes.

        :raises AssertionError: if runtime is already registered or klass is
            not a base class for python processes.
        """
        class_name = klass.__name__
        assert (
            class_name not in self._registered_classes
        ), f"Class {class_name} already registered."
        assert issubclass(
            klass, Process
        ), f"Class {class_name} must be a subclass of resolwe.process.runtime.Process."
        assert (
            "_abstract" in klass.__dict__
        ), f"Class {class_name} must contain as class attribute _abstract."

        module_name = klass.__module__[: klass.__module__.rindex(".")]
        module = import_module(module_name)

        self._registered_classes[klass.__name__] = klass
        self._class_module[klass.__name__] = module
        self._dependencies[klass.__name__] = {
            self._class_module[runtime_class.__name__]
            for runtime_class in self._get_runtime_classes(klass)
        }
        logger.debug(
            "Registered python class %s, module %s.", klass.__name__, module.__name__
        )

    def necessary_modules(self, class_name: str) -> Set[ModuleType]:
        """Get a set of necessary modules for the given class_name."""
        return self._dependencies[class_name]

    def registered_class(self, class_name: str) -> Optional[Type["Process"]]:
        """Get registered class from name."""
        return self._registered_classes.get(class_name)

    def is_registered(self, class_name: str) -> bool:
        """Return if the given class name is registered as runtime class."""
        return class_name in self._registered_classes


python_runtimes_manager = RegisteredRuntimes()
