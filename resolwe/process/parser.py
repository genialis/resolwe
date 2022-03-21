"""Safe Resolwe process parser."""
import ast
import collections
import json
from copy import deepcopy
from inspect import isclass

import asteval

from . import runtime
from .descriptor import Persistence, ProcessDescriptor, SchedulingClass, ValidationError
from .fields import get_available_fields
from .runtimes import python_runtimes_manager


class StaticMetadata:
    """Process metadata item."""

    def get_value(self, node):
        """Convert value from an AST node."""
        raise NotImplementedError


class StaticStringMetadata(StaticMetadata):
    """String metadata item."""

    def get_value(self, node):
        """Convert value from an AST node."""
        if not isinstance(node, ast.Str):
            raise TypeError("must be a string literal")

        return node.s


class StaticBooleanMetadata(StaticMetadata):
    """Boolean metadata item."""

    def get_value(self, node):
        """Convert value from an AST node."""
        if not isinstance(node, (ast.NameConstant, ast.Constant)):
            raise TypeError("must be a constant boolean literal")
        value = getattr(node, "s", node.value)
        if not isinstance(value, bool):
            raise TypeError("must be a constant boolean literal")
        return value


class StaticEnumMetadata(StaticMetadata):
    """Enum metadata item."""

    def __init__(self, choices):
        """Construct enum metadata item."""
        self.choices = choices

    def get_value(self, node):
        """Convert value from an AST node."""
        if not isinstance(node, ast.Attribute):
            raise TypeError("must be an attribute")

        if node.value.id != self.choices.__name__:
            raise TypeError("must be an attribute of {}".format(self.choices.__name__))

        return getattr(self.choices, node.attr)


class StaticDictMetadata(StaticMetadata):
    """Dict metadata item."""

    def get_value(self, node):
        """Convert value from an AST node."""
        if not isinstance(node, ast.Dict):
            raise TypeError("must be a dictionary")

        evaluator = SafeEvaluator()
        try:
            value = evaluator.run(node)
        except Exception as ex:
            # TODO: Handle errors.
            raise ex

        try:
            # Ensure value is a serializable dictionary.
            value = json.loads(json.dumps(value))
            if not isinstance(value, dict):
                raise TypeError
        except (TypeError, ValueError) as exception:
            raise TypeError("must be serializable") from exception

        return value


# Possible process metadata.
PROCESS_METADATA = {
    "slug": StaticStringMetadata(),
    "name": StaticStringMetadata(),
    "process_type": StaticStringMetadata(),
    "version": StaticStringMetadata(),
    "category": StaticStringMetadata(),
    "scheduling_class": StaticEnumMetadata(choices=SchedulingClass),
    "persistence": StaticEnumMetadata(choices=Persistence),
    "requirements": StaticDictMetadata(),
    "data_name": StaticStringMetadata(),
    "entity": StaticDictMetadata(),
    "abstract": StaticBooleanMetadata(),
}


class SafeEvaluator(asteval.Interpreter):
    """Safe evaluator of Python expressions."""

    def __init__(self, symtable=None):
        """Initialize the safe evaluator."""
        if symtable is None:
            # Ensure symbol table is safely initialized as leaving it set to None makes
            # all kinds of builtin functions available.
            symtable = {}

        super().__init__(symtable=symtable, use_numpy=False, no_print=True)


class ProcessVisitor(ast.NodeVisitor):
    """Visitor for parsing process AST."""

    def __init__(self, source):
        """Construct process AST visitor."""
        self.source = source
        self.processes = []
        self.base_classes = set()
        # The defined classes list is a list of all processes defined in the
        # given file we have seen so far.
        # This allows us to define processes that inherit from other processes
        # defined it the same file.
        self.defined_clases = dict()

        super().__init__()

    def visit_field_class(self, item, descriptor=None, fields=None):
        """Visit a class node containing a list of field definitions."""
        discovered_fields = collections.OrderedDict()
        field_groups = {}
        for node in item.body:
            if isinstance(node, ast.ClassDef):
                field_groups[node.name] = self.visit_field_class(node)
                continue

            if not isinstance(node, ast.Assign):
                continue
            if not isinstance(node.value, ast.Call):
                continue
            if not isinstance(node.targets[0], ast.Name):
                continue

            # Build accessible symbols table.
            symtable = {}
            # All field types.
            symtable.update({field.__name__: field for field in get_available_fields()})
            # Field group classes.
            symtable.update(field_groups)

            evaluator = SafeEvaluator(symtable=symtable)

            name = node.targets[0].id
            try:
                field = evaluator.run(node.value)
            except Exception as ex:
                # TODO: Handle errors.
                raise ex

            if descriptor is not None:
                field.contribute_to_class(descriptor, fields, name)
            else:
                discovered_fields[name] = field

        if descriptor is None:

            class Fields:
                """Fields wrapper."""

            for name, field in discovered_fields.items():
                setattr(Fields, name, field)

            return Fields

    def visit_ClassDef(self, node):
        """Visit top-level classes."""
        # Resolve everything as root scope contains everything from the process module.
        # Iterate through all base classes for the given class and determine
        # if it represents the Python process. It represents the process if:
        # * it is derived from one of registered Python runtime classes or
        # * it is derived from one of the processes defined previously in the
        #   same file.
        derived_class = False  # Class is extended from non-base Process class.
        for base in node.bases:
            base_name = ""
            # Cover `from resolwe.process import ...`.
            if isinstance(base, ast.Name) and isinstance(base.ctx, ast.Load):
                base_name = base.id
                base = python_runtimes_manager.registered_class(base.id)
                if base is not None:
                    self.base_classes.add(base.__name__)

            # Cover `from resolwe import process`.
            elif isinstance(base, ast.Attribute) and isinstance(base.ctx, ast.Load):
                base_name = base.attr
                base = python_runtimes_manager.registered_class(base.attr)
                if base is not None:
                    self.base_classes.add(base.__name__)
            else:
                continue

            if isclass(base) and issubclass(base, runtime.Process):
                break

            if base_name in self.defined_clases:
                derived_class = True
                break
        else:
            return

        descriptor = ProcessDescriptor(source=self.source)
        parent_descriptor = self.defined_clases.get(base_name)
        if parent_descriptor is not None:
            # Copy from parent.
            descriptor.metadata.__dict__ = deepcopy(parent_descriptor.metadata.__dict__)
            descriptor.metadata.abstract = False
            descriptor.inputs = deepcopy(parent_descriptor.inputs)
            descriptor.outputs = deepcopy(parent_descriptor.outputs)
            descriptor.relations = deepcopy(parent_descriptor.relations)
            descriptor.parent_process = parent_descriptor.parent_process

        descriptor.metadata.lineno = node.lineno
        descriptor.metadata.description = None

        # Available embedded classes.
        embedded_class_fields = {
            runtime.PROCESS_INPUTS_NAME: descriptor.inputs,
            runtime.PROCESS_OUTPUTS_NAME: descriptor.outputs,
        }

        # The set of explicitely defined metadata attributes in the process.
        defined_metadata_attributes = set()
        # Parse metadata in class body.
        for item in node.body:
            if isinstance(item, ast.Assign):
                # Possible metadata.
                if (
                    len(item.targets) == 1
                    and isinstance(item.targets[0], ast.Name)
                    and isinstance(item.targets[0].ctx, ast.Store)
                    and item.targets[0].id in PROCESS_METADATA
                ):
                    # Try to get the metadata value.
                    metadata_attribute_name = item.targets[0].id
                    value = PROCESS_METADATA[metadata_attribute_name].get_value(
                        item.value
                    )
                    setattr(descriptor.metadata, metadata_attribute_name, value)
                    defined_metadata_attributes.add(metadata_attribute_name)

            elif (
                isinstance(item, ast.Expr)
                and isinstance(item.value, ast.Str)
                and descriptor.metadata.description is None
            ):
                # Possible description string.
                descriptor.metadata.description = item.value.s
            elif (
                isinstance(item, ast.ClassDef)
                and item.name in embedded_class_fields.keys()
            ):
                # Possible input/output declaration. Remove input/output set
                # from the parents classes (if any).
                embedded_class_fields[item.name].clear()
                self.visit_field_class(
                    item, descriptor, embedded_class_fields[item.name]
                )

        # The abstract class must have the version attribute defined.
        if descriptor.metadata.abstract and descriptor.metadata.version is None:
            raise ValidationError(
                f"Abstract process '{node.name}' is missing version attribute."
            )
        # Derived class must have no version attribute defined.
        elif derived_class and "version" in defined_metadata_attributes:
            raise ValidationError(
                f"Derived class '{node.name}' must not define the version attribute."
            )

        # Do not validate and return abstract processes, parts of them may be
        # missing.
        if not descriptor.metadata.abstract:
            descriptor.validate()
            self.processes.append(descriptor)

        # Store descriptor for later use.
        self.defined_clases[node.name] = descriptor


class SafeParser:
    """Safe parser for Python processes which doesn't evaluate any code."""

    def __init__(self, source):
        """Construct process parser.

        :param source: Process source code string
        """
        self._source = source

    def parse(self):
        """Parse process.

        :return: A list of discovered process descriptors
        """
        root = ast.parse(self._source)
        visitor = ProcessVisitor(source=self._source)
        visitor.visit(root)
        return visitor.processes

    def base_classes(self):
        """Parse process.

        :return: A list of the base classes for the processes.
        """

        print(self._source)

        root = ast.parse(self._source)
        visitor = ProcessVisitor(source=self._source)
        visitor.visit(root)
        return visitor.base_classes
