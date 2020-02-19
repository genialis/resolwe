"""Process runtime."""
import json
import logging
import os
import sys
import urllib

import resolwe_runtime_utils

from .descriptor import ProcessDescriptor
from .fields import Field

try:
    from plumbum import local as Cmd

    # Log plumbum commands to standard output.

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s[%(process)s]: %(message)s"
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    plumbum_logger = logging.getLogger("plumbum.local")
    plumbum_logger.setLevel(logging.DEBUG)
    plumbum_logger.addHandler(handler)


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
PROCESS_INPUTS_NAME = "Input"
# Outputs class name.
PROCESS_OUTPUTS_NAME = "Output"


class _IoBase:
    """Base class for input/output values."""

    frozen = False

    def __init__(self, fields, **kwargs):
        """Construct an I/O object."""
        super().__setattr__("_data", {})
        super().__setattr__("_fields", fields)
        self._data.update(kwargs)

    def freeze(self):
        """Freeze the object, preventing further mutation."""
        super().__setattr__("frozen", True)
        return self

    def items(self):
        """Return iterator over I/O items."""
        return self._data.items()

    def __getattr__(self, name):
        return self._data.get(name, None)

    def __setattr__(self, name, value):
        if self.frozen:
            raise ValueError("cannot mutate frozen object")

        if name not in self._fields:
            raise ValueError("invalid field name '{}'".format(name))

        value = self._fields[name].clean(value)
        self._data[name] = value

    def __delattr__(self, name):
        if self.frozen:
            raise ValueError("cannot mutate frozen object")

        try:
            del self._data[name]
        except KeyError:
            raise AttributeError(name)

    def __repr__(self):
        return "<{} {}>".format(
            self.__class__.__name__,
            ", ".join(
                [
                    "{}={}".format(field, repr(value))
                    for field, value in self._data.items()
                ]
            ),
        )


class Inputs(_IoBase):
    """Process inputs."""

    def __getattr__(self, name):
        """Get input field.

        Check that the input exists.
        """
        if name not in self._fields:
            raise AttributeError("Inputs have no field {}".format(name))

        return super().__getattr__(name)


class Outputs(_IoBase):
    """Process outputs."""

    def __getattr__(self, name):
        """Get output field.

        Check that the output exists.
        """
        if name not in self._fields:
            raise AttributeError("Outputs have no field {}".format(name))

        return super().__getattr__(name)

    def __setattr__(self, name, value):
        """Store attribute value and save output."""
        super().__setattr__(name, value)

        output_value = self._fields[name].to_output(self._data[name])
        print(json.dumps(output_value))


class ProcessMeta(type):
    """Metaclass for processes during runtime."""

    def __new__(mcs, name, bases, namespace, **kwargs):
        """Create new process class."""
        if namespace["__module__"] == "resolwe.process.runtime":
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
    """Resolwe process."""

    def __init__(self):
        """Construct a new process instance."""
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, inputs, outputs):
        """Process entry point."""
        raise NotImplementedError

    def run_process(self, slug, inputs):
        """Run a new process from a running process."""

        def export_files(value):
            """Export input files of spawned process."""
            if isinstance(value, str) and os.path.isfile(value):
                # TODO: Use the protocol to export files and get the
                # process schema to check field type.
                print("export {}".format(value))
            elif isinstance(value, dict):
                for item in value.values():
                    export_files(item)
            elif isinstance(value, list):
                for item in value:
                    export_files(item)

        export_files(inputs)
        print(
            "run {}".format(
                json.dumps({"process": slug, "input": inputs}, separators=(",", ":"))
            )
        )

    def progress(self, progress):
        """Report process progress.

        :param progress: A float between 0 and 1 denoting the progress
        """
        report = resolwe_runtime_utils.progress(progress)
        # TODO: Use the protocol to report progress.
        print(report)

    def info(self, *args):
        """Log informational message."""
        report = resolwe_runtime_utils.info(" ".join([str(x) for x in args]))
        # TODO: Use the protocol to report progress.
        print(report)

    def warning(self, *args):
        """Log warning message."""
        report = resolwe_runtime_utils.warning(" ".join([str(x) for x in args]))
        # TODO: Use the protocol to report progress.
        print(report)

    def error(self, *args):
        """Log error message."""
        report = resolwe_runtime_utils.error(" ".join([str(x) for x in args]))
        # TODO: Use the protocol to report progress.
        print(report)

    def get_data_id_by_slug(self, slug):
        """Find data object ID for given slug.

        This method queries the Resolwe API and requires network access.
        """
        resolwe_host = os.environ.get("RESOLWE_HOST_URL")
        url = urllib.parse.urljoin(
            resolwe_host, "/api/data?slug={}&fields=id".format(slug)
        )

        with urllib.request.urlopen(url, timeout=60) as f:
            data = json.loads(f.read().decode("utf-8"))

        if len(data) == 1:
            return data[0]["id"]
        elif not data:
            raise ValueError("Data not found for slug {}".format(slug))
        else:
            raise ValueError(
                "More than one data object returned for slug {}".format(slug)
            )

    def update_entity_descriptor(self, annotations):
        """Update entity descriptor from dictionary of annotations."""
        for key, value in annotations.items():
            print(resolwe_runtime_utils.annotate_entity(key, value))

    @property
    def requirements(self):
        """Process requirements."""

        class dotdict(dict):
            """Dot notation access to dictionary attributes."""

            def __getattr__(self, attr):
                value = self.get(attr)
                return dotdict(value) if isinstance(value, dict) else value

        return dotdict(self._meta.metadata.requirements)

    def start(self, inputs):
        """Start the process.

        :param inputs: An instance of `Inputs` describing the process inputs
        :return: An instance of `Outputs` describing the process outputs
        """
        self.logger.info("Process is starting")

        outputs = Outputs(self._meta.outputs)

        self.logger.info("Process is running")
        try:
            self.run(inputs.freeze(), outputs)
            return outputs.freeze()
        except Exception as error:
            self.logger.exception("Exception while running process")
            print(resolwe_runtime_utils.error(str(error)))
            raise
        except:  # noqa
            self.logger.exception("Exception while running process")
            print(resolwe_runtime_utils.error("Exception while running process"))
            raise
        finally:
            self.logger.info("Process has finished")
