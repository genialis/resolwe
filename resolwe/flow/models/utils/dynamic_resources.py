"""Utility functions for dynamic calculation of process resource requirements."""

import ast
import dis
import math
from functools import reduce

from resolwe.flow.models import Data, Process


def _eval_dynamic_requirements(expr: str, variables: dict):
    """Evaluate an arithmetic expression."""
    allowed_codes = {
        "RESUME",  # noop
        "BINARY_OP",  # infix binary op
        "BINARY_ADD",  # +
        "BINARY_SUBTRACT",  # -
        "BINARY_MULTIPLY",  # *
        "BINARY_TRUE_DIVIDE",  # /
        "BINARY_FLOOR_DIVIDE",  # //
        "BINARY_MODULO",  # %
        "BINARY_POWER",  # **
        "COMPARE_OP",  # == > < >= <=
        "UNARY_NOT",  # not
        "LOAD_CONST",  # constants
        "LOAD_NAME",  # variables
        "CALL_FUNCTION",  # provided functions, e.g. log
        "RETURN_VALUE",  # implicit return
        "RETURN_CONST",  # implicit return (alt.)
    }
    try:
        compiled = compile(expr, "", "eval")
    except SyntaxError:
        raise ValueError(f"{expr} is not a valid expression")

    for instruction in dis.get_instructions(expr):
        opcode = dis.opname[instruction.opcode]
        if opcode not in allowed_codes:
            raise ValueError(f"Operation {opcode} not allowed (in {expr})")

    scope = {"log": math.log, "exp": math.exp, "sqrt": math.sqrt}
    scope.update(variables)
    scope.update({"__builtins__": {}})
    return eval(compiled, scope)


def schema_get(schema: list, key: str) -> dict | list:
    """Return a schema field with the given name."""
    field = next(iter(filter(lambda field: field["name"] == key, schema)))
    if "group" in field:
        return field["group"]
    return field


def _resolve_value(name: str, input_values: dict, input_schema: list):
    """Resolve the value of a variable in the expression."""

    try:
        keys = name.split("__")
        value = reduce(dict.get, keys, input_values)
        type_ = reduce(schema_get, keys, input_schema)["type"]
    except (KeyError, StopIteration):
        raise ValueError(f"Unrecognized variable {name}")

    # More types may be supported as needed.
    if type_ == "data:":
        return Data.objects.get(pk=value).size
    elif type_ in ("basic:integer:", "basic:float:", "basic:boolean:"):
        return value

    raise ValueError(f"Unsupported type {type_}")


def get_dynamic_resource_limits(process: Process, data: Data):
    """Get the dynamic resource requirements for this process."""
    resources = {}

    # Get the resource requirements from the process.
    requirements = process.requirements.get("resources", {})
    for resource, formula in requirements.items():
        if isinstance(formula, (int, float)):
            resources[resource] = formula
            continue

        # Get variables used in the expression.
        used_variables = {
            node.id for node in ast.walk(ast.parse(formula)) if type(node) is ast.Name
        }

        # Find the values of the used variables.
        try:
            variables = {
                var: _resolve_value(var, data.input, process.input_schema)
                for var in used_variables
            }
        except ValueError as exc:
            raise ValueError(f"{exc} in requirements for {resource}: {formula}")

        amount = _eval_dynamic_requirements(process, formula, variables)
        if amount is not None:
            resources[resource] = amount

    return resources
