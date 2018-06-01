"""Process runtime."""
import logging

import resolwe_runtime_utils

from .descriptor import *  # pylint: disable=wildcard-import,unused-wildcard-import
from .fields import *  # pylint: disable=wildcard-import,unused-wildcard-import

try:
    from plumbum import local as Cmd  # pylint: disable=unused-import
except ImportError:
    # Generate a dummy class that defers the ImportError to a point where
    # the package actually tries to be used.
    class MissingPlumbumCmd:
        """Missing plumbum command dummy class."""

        def __getitem__(self, cmd):
            """Raise ImportError exception."""
            raise ImportError("Missing 'plumbum' Python package.")

    Cmd = MissingPlumbumCmd()

# Inputs class name.
PROCESS_INPUTS_NAME = 'Input'
# Outputs class name.
PROCESS_OUTPUTS_NAME = 'Output'


class _IoBase:
    """Base class for input/output values."""

    frozen = False

    def __init__(self, **kwargs):
        """Construct an I/O object."""
        super().__setattr__('_data', {})
        self._data.update(kwargs)

    def freeze(self):
        """Freeze the object, preventing further mutation."""
        super().__setattr__('frozen', True)
        return self

    def items(self):
        """Return iterator over I/O items."""
        return self._data.items()

    def __getattr__(self, name):
        try:
            return self._data[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        if self.frozen:
            raise ValueError("cannot mutate frozen object")

        self._data[name] = value

    def __delattr__(self, name):
        if self.frozen:
            raise ValueError("cannot mutate frozen object")

        try:
            del self._data[name]
        except KeyError:
            raise AttributeError(name)

    def __repr__(self):
        return '<{} {}>'.format(
            self.__class__.__name__,
            ', '.join([
                '{}={}'.format(field, repr(value))
                for field, value in self._data.items()
            ]),
        )


class Inputs(_IoBase):
    """Process inputs."""

    pass


class Outputs(_IoBase):
    """Process outputs."""

    pass


class ProcessMeta(type):
    """Metaclass for processes during runtime."""

    def __new__(mcs, name, bases, namespace, **kwargs):
        """Create new process class."""
        if namespace['__module__'] == 'resolwe.process.runtime':
            return type.__new__(mcs, name, bases, namespace)

        # Generate a runtime version of the process descriptor.
        meta = ProcessDescriptor()
        for meta_name in dir(meta.metadata):
            if meta_name.startswith('_'):
                continue

            setattr(meta.metadata, meta_name, namespace.pop(meta_name, None))

        for nsp, fields in ((PROCESS_INPUTS_NAME, meta.inputs), (PROCESS_OUTPUTS_NAME, meta.outputs)):
            inputs = namespace.pop(nsp, {})
            for field_name in dir(inputs):
                if field_name.startswith('_'):
                    continue

                field = getattr(inputs, field_name)
                if not isinstance(field, Field):
                    continue
                field.contribute_to_class(meta, fields, field_name)

        meta.validate()

        result = type.__new__(mcs, name, bases, namespace)
        result._meta = meta  # pylint: disable=protected-access
        return result


class Process(metaclass=ProcessMeta):
    """Resolwe process."""

    def __init__(self):
        """Construct a new process instance."""
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, inputs, outputs):
        """Process entry point."""
        raise NotImplementedError

    def progress(self, progress):
        """Report process progress.

        :param progress: A float between 0 and 1 denoting the progress
        """
        report = resolwe_runtime_utils.progress(progress)
        # TODO: Use the protocol to report progress.
        print(report)

    def info(self, *args):
        """Log informational message."""
        report = resolwe_runtime_utils.info(' '.join([str(x) for x in args]))
        # TODO: Use the protocol to report progress.
        print(report)

    def warning(self, *args):
        """Log warning message."""
        report = resolwe_runtime_utils.warning(' '.join([str(x) for x in args]))
        # TODO: Use the protocol to report progress.
        print(report)

    def error(self, *args):
        """Log error message."""
        report = resolwe_runtime_utils.error(' '.join([str(x) for x in args]))
        # TODO: Use the protocol to report progress.
        print(report)

    def update_descriptor(self, **kwargs):
        """Update sample descriptor."""
        raise NotImplementedError

    def start(self, inputs):
        """Start the process.

        :param inputs: An instance of `Inputs` describing the process inputs
        :return: An instance of `Outputs` describing the process outputs
        """
        # pylint: disable=logging-format-interpolation
        self.logger.info("Process is starting")

        outputs = Outputs()

        # Validate and clean inputs.
        try:
            inputs = self._validate_inputs(inputs)
        except ValidationError as error:
            self.logger.error("Input validation has failed: {}".format(error.args[0]))
            raise

        self.logger.info("Process is running")
        try:
            self.run(inputs.freeze(), outputs)
            try:
                outputs = self._validate_outputs(outputs)
            except ValidationError as error:
                self.logger.error("Output validation has failed: {}".format(error.args[0]))
                raise

            return outputs.freeze()
        except:  # noqa pylint: disable=bare-except
            self.logger.exception("Exception while running process")
            raise
        finally:
            self.logger.info("Process has finished")

    def _validate_inputs(self, inputs):
        """Validate process inputs."""
        valid_inputs = Inputs()
        for name, field in self._meta.inputs.items():
            value = getattr(inputs, name, None)
            setattr(valid_inputs, name, field.clean(value))

        return valid_inputs

    def _validate_outputs(self, outputs):
        """Validate process outputs."""
        valid_outputs = Outputs()
        for name, field in self._meta.outputs.items():
            value = getattr(outputs, name, None)
            value = field.clean(value)
            value = field.to_output(value)
            setattr(valid_outputs, name, value)

        return valid_outputs
