"""Jinja2-based expression engine."""
from __future__ import absolute_import, division, print_function, unicode_literals

from importlib import import_module

import jinja2

from django.core.exceptions import ImproperlyConfigured

from resolwe.flow.expression_engines.base import BaseExpressionEngine
from resolwe.flow.expression_engines.exceptions import EvaluationError

from .filters import filters as builtin_filters


class NestedUndefined(jinja2.Undefined):
    """An undefined variable that can be nested."""

    def __unicode__(self):
        """Override."""
        return ''

    def __str__(self):
        """Override."""
        return ''

    def __call__(self, *args, **kwargs):
        """Override."""
        return NestedUndefined()

    def __getattr__(self, *args, **kwargs):
        """Override."""
        return NestedUndefined()

    def __getitem__(self, *args, **kwargs):
        """Override."""
        return NestedUndefined()


def propagate_errors_as_undefined(function):
    """Propagate exceptions as undefined values filter."""
    def wrapper(*args, **kwargs):
        """Filter wrapper."""
        try:
            return function(*args, **kwargs)
        except Exception:  # pylint: disable=broad-except
            return NestedUndefined()

    # Copy over Jinja filter decoration attributes.
    for attribute in dir(function):
        if attribute.endswith('filter'):
            setattr(wrapper, attribute, getattr(function, attribute))

    return wrapper


class ExpressionEngine(BaseExpressionEngine):
    """Jinja2-based expression engine."""

    name = 'jinja'
    inline_tags = ('{{', '}}')

    def __init__(self, *args, **kwargs):
        """Construct the expression engine."""
        super(ExpressionEngine, self).__init__(*args, **kwargs)

        # Initialize the Jinja2 environment.
        # TODO: Should we use a sandboxed environment?
        self._environment = jinja2.Environment(
            undefined=NestedUndefined
        )
        # Register built-in filters.
        self._environment.filters.update(builtin_filters)
        # Register custom filters.
        self._register_custom_filters()
        # Decorate all filters with undefined error propagation.
        for name, function in self._environment.filters.items():
            self._environment.filters[name] = propagate_errors_as_undefined(function)

    def _register_custom_filters(self):
        """Register any custom filter modules."""
        custom_filters = self.settings.get('CUSTOM_FILTERS', [])
        if not isinstance(custom_filters, list):
            raise KeyError("`CUSTOM_FILTERS` setting must be a list.")

        for filter_module_name in custom_filters:
            try:
                filter_module = import_module(filter_module_name)
            except ImportError as error:
                raise ImproperlyConfigured(
                    "Failed to load custom filter module '{}'.\n"
                    "Error was: {}".format(filter_module_name, error)
                )

            try:
                filter_map = getattr(filter_module, 'filters')
                if not isinstance(filter_map, dict):
                    raise TypeError
            except (AttributeError, TypeError):
                raise ImproperlyConfigured(
                    "Filter module '{}' does not define a 'filters' dictionary".format(filter_module_name)
                )
            self._environment.filters.update(filter_map)

    def evaluate_block(self, template, context=None):
        """Evaluate a template block."""
        if context is None:
            context = {}

        try:
            template = self._environment.from_string(template)
            return template.render(**context)
        except jinja2.TemplateError as error:
            raise EvaluationError(error.args[0])

    def evaluate_inline(self, expression, context=None):
        """Evaluate an inline expression."""
        if context is None:
            context = {}

        try:
            compiled = self._environment.compile_expression(expression)
            return compiled(**context)
        except jinja2.TemplateError as error:
            raise EvaluationError(error.args[0])
