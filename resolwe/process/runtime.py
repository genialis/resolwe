"""Process runtime."""
import logging
import sys
from pathlib import Path
from typing import Dict, List, Union

from .communicator import communicator
from .descriptor import ProcessDescriptor
from .fields import Field, RelationDescriptor
from .models import Data, JSONDescriptor

# Inputs class name.
PROCESS_INPUTS_NAME = "Input"
# Outputs class name.
PROCESS_OUTPUTS_NAME = "Output"
logger = logging.getLogger(__name__)


class FlushOnWrite:
    """Flush stream after every write.

    This is a middle ground between buffered stdout/stderr Python uses when it
    is connected to a pipe and unbuffered stdout/stderrr. The first one couses
    that stdout in not updated in real time (and lost on crashes) and the
    second solution causes stdout update for every character.
    """

    def __init__(self, stream):
        """Store the stream for later use."""
        self.stream = stream

    def write(self, data):
        """Write data to the stream and flush it."""
        self.stream.write(data)
        self.stream.flush()

    def writelines(self, datas):
        """Write lines to the stream and flush it."""
        self.stream.writelines(datas)
        self.stream.flush()

    def __getattr__(self, attr):
        """Redirect other attribute lookups to the underlying stream."""
        return getattr(self.stream, attr)


sys.stdout = FlushOnWrite(sys.stdout)  # type: ignore
sys.stderr = FlushOnWrite(sys.stderr)  # type: ignore


class ProcessMeta(type):
    """Metaclass for process.

    It is used to construct ProcessDescriptor for each class that inherites
    from Process. That information is used to generate the process schema.
    """

    def __new__(mcs, name, bases, namespace, **kwargs):
        """Create new process class."""
        # If this an abstract process (ie the parent class for real processes)
        # construct and return its instance immediatelly.
        if "_abstract" in namespace:
            return type.__new__(mcs, name, bases, namespace)

        # Generate a runtime version of the process descriptor.
        meta = ProcessDescriptor()
        for meta_name in dir(meta.metadata):
            if meta_name.startswith("_"):
                continue

            setattr(meta.metadata, meta_name, namespace.pop(meta_name, None))

        for nsp, fields in (
            (PROCESS_INPUTS_NAME, meta.inputs),
            (PROCESS_OUTPUTS_NAME, meta.outputs),
        ):
            inputs = namespace.pop(nsp, {})
            for field_name in dir(inputs):
                if field_name.startswith("_"):
                    continue

                field = getattr(inputs, field_name)
                if not isinstance(field, Field):
                    continue
                field.contribute_to_class(meta, fields, field_name)

        meta.validate()

        result = type.__new__(mcs, name, bases, namespace)
        result._meta = meta
        return result


class Process(metaclass=ProcessMeta):
    """Resolwe process.

    This is the base class from which all other python processes must inherit.
    When one wants to create an abstract python process (a process which is
    not automatically registered in the Process database table) the class
    attribute `_abstract` must be set to True.
    """

    # Mark it an abstract process: such processes are not registered in the
    # database and are parents for actual processes.
    # Make sure to add this class property to all process parent classes.
    _abstract = True

    def __init__(self, data: Data):
        """Construct a new process instance."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.data = data
        self.process = self.data.process

    def run(self, inputs: JSONDescriptor, outputs: JSONDescriptor):
        """Process entry point."""
        raise NotImplementedError

    def export_files(self, exports: Union[str, List, Dict]):
        """Export files."""

        def file_list(value):
            values = []
            if isinstance(value, str) and Path(value).is_file():
                # TODO: Use the protocol to export files and get the
                # process schema to check field type.
                values = [value]
            elif isinstance(value, dict):
                values = []
                for item in value.values():
                    values += file_list(item)
            elif isinstance(value, list):
                values = []
                for item in value:
                    values += file_list(item)
            return values

        files = file_list(exports)
        communicator.export_files(files)

    def run_process(self, slug: str, inputs: Dict):
        """Run a new process from a running process."""
        self.export_files(inputs)
        communicator.run({"process": slug, "input": inputs})

    def progress(self, progress: float):
        """Report process progress.

        :raises AssertionError: when float is not between 0 and 1.
        """
        assert 0 <= progress <= 1, "Progress must be a float between 0 and 1."
        communicator.progress(round(progress * 100))

    def _process_log(self, log: Dict[str, List[str]]):
        """Send process log.

        The log may contain multiple info, warning and error messages.

        :param log: dictionary with keys 'info', 'warning' and 'error'. The
            corresponding values are lists of strings. Some keys may be
            missing.
        """
        communicator.process_log(log)

    def info(self, *args):
        """Log informational message."""
        to_send = " ".join([str(x) for x in args])
        self._process_log({"info": [to_send]})

    def warning(self, *args):
        """Log warning message."""
        to_send = " ".join([str(x) for x in args])
        self._process_log({"warning": [to_send]})

    def error(self, *args):
        """Log error message."""
        to_send = " ".join([str(x) for x in args])
        self._process_log({"error": [to_send]})

    def get_data_id_by_slug(self, slug) -> int:
        """Find data object ID for given slug."""
        return Data.from_slug(slug).id

    def update_entity_descriptor(self, annotations: dict):
        """Update entity descriptor from dictionary of annotations."""
        communicator.annotate(annotations)

    @property
    def requirements(self):
        """Process requirements."""

        class dotdict(dict):
            """Dot notation access to dictionary attributes."""

            def __getattr__(self, attr):
                value = self.get(attr)
                return dotdict(value) if isinstance(value, dict) else value

        return dotdict(communicator.get_self_requirements(self.data.process.id))

    @property
    def name(self):
        """Get Data name."""
        return self.data.name

    @property
    def relations(self):
        """Process relations."""
        relations = communicator.get_relations(self.data.id)
        return [RelationDescriptor.from_dict(data) for data in relations]

    def start(self):
        """Start the process.

        :param inputs: An instance of `Inputs` describing the process inputs
        :return: An instance of `Outputs` describing the process outputs
        """
        self.logger.info("Process is starting")
        try:
            self.run(self.data.input, self.data.output)
            return self.data.output.freeze()
        except Exception as error:
            self.logger.exception("Exception while running process")
            self.error(str(error))
            raise
        except:  # noqa
            self.logger.exception("Exception while running process")
            self.error("Exception while running process")
            raise
        finally:
            self.logger.info("Process has finished")
