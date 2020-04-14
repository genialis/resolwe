"""Safe Resolwe process parser."""
import ast
import collections
import json
from inspect import isclass

import asteval

from . import runtime
from .descriptor import Persistence, ProcessDescriptor, SchedulingClass
from .fields import get_available_fields


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
        except (TypeError, ValueError):
            raise TypeError("must be serializable")

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
}


class SafeEvaluator(asteval.Interpreter):
    """Safe evaluator of Python expressions."""

    def __init__(self, symtable=None):
        """Initialize the safe evaluator."""
        if symtable is None:
            # Ensure symbol table is safely initialized as leaving it set to None makes
            # all kinds of builtin functions available.
            symtable = {}

        super().__init__(
            symtable=symtable, use_numpy=False, no_print=True,
        )


class ProcessVisitor(ast.NodeVisitor):
    """Visitor for parsing process AST."""

    def __init__(self, source):
        """Construct process AST visitor."""
        self.source = source
        self.processes = []

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
        for base in node.bases:
            # Cover `from resolwe.process import ...`.
            if isinstance(base, ast.Name) and isinstance(base.ctx, ast.Load):
                base = getattr(runtime, base.id, None)
            # Cover `from resolwe import process`.
            elif isinstance(base, ast.Attribute) and isinstance(base.ctx, ast.Load):
                base = getattr(runtime, base.attr, None)
            else:
                continue

            if isclass(base) and issubclass(base, runtime.Process):
                break
        else:
            return

        descriptor = ProcessDescriptor(source=self.source)

        # Available embedded classes.
        embedded_class_fields = {
            runtime.PROCESS_INPUTS_NAME: descriptor.inputs,
            runtime.PROCESS_OUTPUTS_NAME: descriptor.outputs,
        }

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
                    value = PROCESS_METADATA[item.targets[0].id].get_value(item.value)
                    setattr(descriptor.metadata, item.targets[0].id, value)
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
                # Possible input/output declaration.
                self.visit_field_class(
                    item, descriptor, embedded_class_fields[item.name]
                )

        descriptor.validate()
        self.processes.append(descriptor)


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
