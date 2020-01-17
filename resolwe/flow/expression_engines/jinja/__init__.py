"""Jinja2-based expression engine."""
from contextlib import contextmanager
from importlib import import_module

import jinja2
from jinja2 import compiler
from jinja2.utils import soft_unicode

from django.core.exceptions import ImproperlyConfigured

from resolwe.flow.expression_engines.base import BaseExpressionEngine
from resolwe.flow.expression_engines.exceptions import EvaluationError

from .filters import filters as builtin_filters


class NestedUndefined(jinja2.Undefined):
    """An undefined variable that can be nested."""

    def __unicode__(self):
        """Override."""
        return ""

    def __str__(self):
        """Override."""
        return ""

    def __call__(self, *args, **kwargs):
        """Override."""
        return NestedUndefined()

    def __getattr__(self, *args, **kwargs):
        """Override."""
        return NestedUndefined()

    def __getitem__(self, *args, **kwargs):
        """Override."""
        return NestedUndefined()


class SafeCodeGenerator(compiler.CodeGenerator):
    """Code generator wrapper."""

    def write_commons(self):
        """Override common preamble to inject our own escape wrappers."""
        super().write_commons()

        self.writeline("escape = environment.escape")
        self.writeline("Markup = environment.markup_class")


class Environment(jinja2.Environment):
    """Custom Jinja2 environment."""

    # Custom code generator, which allows changing escape and markup.
    code_generator_class = SafeCodeGenerator

    # We usually don't output HTML so we ignore the markup class.
    markup_class = soft_unicode

    def __init__(self, engine):
        """Construct custom environment."""
        self._engine = engine

        super().__init__(
            undefined=NestedUndefined,
            autoescape=lambda _: self._engine._escape is not None,
        )

    def escape(self, value):
        """Escape given value."""
        value = soft_unicode(value)

        if self._engine._escape is None:
            return value

        return self._engine._escape(value)


class ExpressionEngine(BaseExpressionEngine):
    """Jinja2-based expression engine."""

    name = "jinja"
    inline_tags = ("{{", "}}")

    def __init__(self, *args, **kwargs):
        """Construct the expression engine."""
        super().__init__(*args, **kwargs)

        # Initialize the Jinja2 environment.
        # TODO: Should we use a sandboxed environment?
        self._environment = Environment(self)
        # Register built-in filters.
        self._environment.filters.update(builtin_filters)
        # Register custom filters.
        self._register_custom_filters()
        # Override the safe filter.
        self._environment.filters["safe"] = self._filter_mark_safe

        # Decorate all filters with our wrapper.
        for name, function in self._environment.filters.items():
            self._environment.filters[name] = self._wrap_jinja_filter(function)

        # Escape function and safe wrapper.
        self._escape = None
        self._safe_wrapper = None

    def _filter_mark_safe(self, value):
        """Filter to mark a value as safe."""
        if self._safe_wrapper is None:
            return value

        return self._safe_wrapper(value)

    def _wrap_jinja_filter(self, function):
        """Propagate exceptions as undefined values filter."""

        def wrapper(*args, **kwargs):
            """Filter wrapper."""
            try:
                return function(*args, **kwargs)
            except Exception:
                return NestedUndefined()

        # Copy over Jinja filter decoration attributes.
        for attribute in dir(function):
            if attribute.endswith("filter"):
                setattr(wrapper, attribute, getattr(function, attribute))

        return wrapper

    def _register_custom_filters(self):
        """Register any custom filter modules."""
        custom_filters = self.settings.get("CUSTOM_FILTERS", [])
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
                filter_map = getattr(filter_module, "filters")
                if not isinstance(filter_map, dict):
                    raise TypeError
            except (AttributeError, TypeError):
                raise ImproperlyConfigured(
                    "Filter module '{}' does not define a 'filters' dictionary".format(
                        filter_module_name
                    )
                )
            self._environment.filters.update(filter_map)

    @contextmanager
    def _evaluation_context(self, escape, safe_wrapper):
        """Configure the evaluation context."""
        self._escape = escape
        self._safe_wrapper = safe_wrapper

        try:
            yield
        finally:
            self._escape = None
            self._safe_wrapper = None

    def evaluate_block(self, template, context=None, escape=None, safe_wrapper=None):
        """Evaluate a template block."""
        if context is None:
            context = {}

        try:
            with self._evaluation_context(escape, safe_wrapper):
                template = self._environment.from_string(template)
                return template.render(**context)
        except jinja2.TemplateError as error:
            raise EvaluationError(error.args[0])
        finally:
            self._escape = None

    def evaluate_inline(self, expression, context=None, escape=None, safe_wrapper=None):
        """Evaluate an inline expression."""
        if context is None:
            context = {}

        try:
            with self._evaluation_context(escape, safe_wrapper):
                compiled = self._environment.compile_expression(expression)
                return compiled(**context)
        except jinja2.TemplateError as error:
            raise EvaluationError(error.args[0])
