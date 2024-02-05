"""Process descriptor."""

import collections
import re

# Processor type validation regexp.
PROCESSOR_TYPE_RE = re.compile(r"^data:[a-z0-9:]+$")


class SchedulingClass:
    """Process scheduling class."""

    INTERACTIVE = "interactive"
    BATCH = "batch"


class Persistence:
    """Process data persistance."""

    RAW = "RAW"
    CACHED = "CACHED"
    TEMP = "TEMP"


class ValidationError(Exception):
    """Process descriptor validation error."""


class ProcessDescriptor:
    """Process descriptor.

    The process descriptor is a result of parsing a process definition. It contains
    everything that is required for registering the process into the database.
    """

    class Metadata:
        """General process metadata."""

        slug = None
        name = None
        process_type = None
        description = None
        version = None
        category = None
        scheduling_class = None
        persistence = None
        requirements = None
        data_name = None
        entity = None
        lineno = None

        def __repr__(self):
            """Return string representation."""
            return repr(self.__dict__)

    def __init__(self, source=None, parent_process=None):
        """Construct a new process descriptor."""
        self.metadata = ProcessDescriptor.Metadata()
        self.inputs = collections.OrderedDict()
        self.outputs = collections.OrderedDict()
        self.relations = None
        self.name = None
        self.source = source
        self.parent_process = parent_process

    def validate(self):
        """Validate process descriptor."""
        required_fields = ("slug", "name", "process_type", "version")
        for field in required_fields:
            if getattr(self.metadata, field, None) is None:
                raise ValidationError(
                    "process '{}' is missing required meta attribute: {}".format(
                        self.metadata.slug or "<unknown>", field
                    )
                )

        if not PROCESSOR_TYPE_RE.match(self.metadata.process_type):
            raise ValidationError(
                "process '{}' has invalid type: {}".format(
                    self.metadata.slug, self.metadata.process_type
                )
            )

    def to_schema(self):
        """Return process schema for this process."""
        process_type = self.metadata.process_type
        if not process_type.endswith(":"):
            process_type = "{}:".format(process_type)

        schema = {
            "slug": self.metadata.slug,
            "name": self.metadata.name,
            "type": process_type,
            "version": self.metadata.version,
            "data_name": "",
            "requirements": {
                "executor": {
                    "docker": {
                        "image": "public.ecr.aws/s4q6j6e8/resolwe/base:ubuntu-18.04",
                    },
                },
            },
        }

        if self.metadata.description is not None:
            schema["description"] = self.metadata.description
        if self.metadata.category is not None:
            schema["category"] = self.metadata.category
        if self.metadata.scheduling_class is not None:
            schema["scheduling_class"] = self.metadata.scheduling_class
        if self.metadata.persistence is not None:
            schema["persistence"] = self.metadata.persistence
        if self.metadata.requirements is not None:
            schema["requirements"] = self.metadata.requirements
        if self.metadata.data_name is not None:
            schema["data_name"] = self.metadata.data_name
        if self.metadata.entity is not None:
            schema["entity"] = self.metadata.entity

        if self.inputs:
            schema["input"] = []
            for field in self.inputs.values():
                schema["input"].append(field.to_schema())

        if self.outputs:
            schema["output"] = []
            for field in self.outputs.values():
                schema["output"].append(field.to_schema())

        schema["run"] = {
            "language": "python",
            "program": self.source or "",
        }

        return schema

    def __repr__(self):
        """Return string representation."""
        return "<ProcessDescriptor metadata={} inputs={} outputs={}>".format(
            repr(self.metadata),
            repr(dict(self.inputs)),
            repr(dict(self.outputs)),
        )
