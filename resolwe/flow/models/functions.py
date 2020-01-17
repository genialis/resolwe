"""Resolwe model database functions."""
from django.db.models.aggregates import Func

from .utils import json_path_components


class JsonGetPath(Func):
    """PostgreSQL JSON path (#>) operator."""

    function = "#>"
    template = "%(expressions)s%(function)s%%s"
    arity = 1

    def __init__(self, expression, path):
        """Initialize function.

        :param expression: Expression that returns a JSON field
        :param path: Path to get inside the JSON object, which can be
            either a list of path components or a dot-separated
            string
        """
        self.path = json_path_components(path)

        super().__init__(expression)

    def as_sql(self, compiler, connection):
        """Compile SQL for this function."""
        sql, params = super().as_sql(compiler, connection)
        params.append(self.path)
        return sql, params


class JsonbArrayElements(Func):
    """PostgreSQL jsonb_array_elements function."""

    function = "jsonb_array_elements"
    template = "%(function)s(%(expressions)s)"
    arity = 1
