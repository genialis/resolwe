"""Safe Resolwe process parser."""

import ast
import collections
import json
from collections import defaultdict
from contextlib import suppress
from inspect import isclass
from typing import Iterable, Mapping, Set

import asteval
from django.conf import settings

from . import runtime
from .descriptor import Persistence, ProcessDescriptor, SchedulingClass
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
        # Mapping betwwen process slug and AST node representing the class.
        self.ast_nodes = dict()

        super().__init__()

    def _get_called_slug(self, process_slug, called_node: ast.Call):
        """Get the slug of the process being called.

        :raises RuntimeError: when slug could not be determined.
        """
        # Both methods (run_process and get_latest) take argument slug as the first
        # argument, so no separation is necessary.
        # When the first non-keyword argumet is given it must be slug.
        keyword_map = {keyword.arg: keyword.value for keyword in called_node.keywords}
        slug = (
            called_node.args[0]
            if len(called_node.args) >= 1
            else keyword_map.get("slug")
        )
        if isinstance(slug, (ast.Constant, ast.Str)):
            return getattr(slug, "value", slug.s), ast.Constant

        if isinstance(slug, ast.Name) and isinstance(slug.ctx, ast.Load):
            return slug.id, ast.Name

        raise RuntimeError(
            f"Spaned process slug could not be determined for the process {process_slug}."
        )

    def get_possible_variable_values(
        self, process_node: ast.ClassDef
    ) -> Mapping[str, Iterable[str]]:
        """Get the mapping between variable names and their possible values.

        The variable is only included in the mapping when it is assigned only
        constant values.
        """
        mapping = defaultdict(list)
        unknown_values: Set[str] = set()
        for child_node in ast.walk(process_node):
            if isinstance(child_node, ast.Assign):
                if isinstance(child_node.value, (ast.Constant, ast.Str)):
                    for target in child_node.targets:
                        if isinstance(target, ast.Name):
                            mapping[target.id].append(
                                getattr(child_node.value, "value", child_node.value.s)
                            )
                else:
                    unknown_values.update(
                        target.id
                        for target in child_node.targets
                        if isinstance(target, ast.Name)
                    )
        # Remove the variables with non-constant assignment from the mapping.
        for unknown in unknown_values:
            mapping.pop(unknown, None)
        return mapping

    def get_dependencies(self):
        """Get the dependencies between the processes.

        :raises RuntimeError: when dependencies could not be determined. For
            instance when process slug is a variable.

        :returns: the dictionary where key is the process slug and the
            corresponding value is the list of processes the process can spawn.
        """
        slugs = dict()
        # Register the runtime with the full name and only the class name.
        # For example "resolwe.process.runtime.Process" and "Process".
        candidate_names = ["self.run_process"]
        for candidate in settings.FLOW_PROCESSES_RUNTIMES:
            candidate_names.append(f"{candidate}.get_latest")
            candidate_names.append(f"{candidate}.get")
            candidate_names.append(f"{candidate.split('.')[-1]}.get_latest")
            candidate_names.append(f"{candidate.split('.')[-1]}.get")
        for process_slug, process_node in self.ast_nodes.items():
            mapping = self.get_possible_variable_values(process_node)
            slugs[process_slug] = set()
            for child_node in ast.walk(process_node):
                if isinstance(child_node, ast.Call):
                    with suppress(AttributeError):
                        candidate_name = (
                            f"{child_node.func.value.id}.{child_node.func.attr}"
                        )
                        if candidate_name in candidate_names:
                            called_slug, slug_type = self._get_called_slug(
                                process_slug, child_node
                            )
                            # The slug is a variable. Collect its possible names.
                            if slug_type == ast.Name:
                                if called_slug not in mapping:
                                    raise RuntimeError(
                                        f"Unable to determine value for the "
                                        f"variable '{called_slug}' in the "
                                        f"process '{process_slug}'."
                                    )
                                slugs[process_slug].update(mapping[called_slug])
                            if slug_type == ast.Constant:
                                slugs[process_slug].add(called_slug)
        return slugs

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
                base = python_runtimes_manager.registered_class(base.id)
                if base is not None:
                    self.base_classes.add(base.__name__)

            # Cover `from resolwe import process`.
            elif isinstance(base, ast.Attribute) and isinstance(base.ctx, ast.Load):
                base = python_runtimes_manager.registered_class(base.attr)
                if base is not None:
                    self.base_classes.add(base.__name__)
            else:
                continue

            if isclass(base) and issubclass(base, runtime.Process):
                break
        else:
            return

        descriptor = ProcessDescriptor(source=self.source)
        descriptor.metadata.lineno = node.lineno

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
        # Add mapping between the slug and the node.
        self.ast_nodes[descriptor.metadata.slug] = node


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
        root = ast.parse(self._source)
        visitor = ProcessVisitor(source=self._source)
        visitor.visit(root)
        return visitor.base_classes
