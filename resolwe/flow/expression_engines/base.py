"""Workflow expression engines."""
from resolwe.flow.engine import BaseEngine


class BaseExpressionEngine(BaseEngine):
    """A workflow expression engine."""

    inline_tags = None

    def get_inline_expression(self, text):
        """Extract an inline expression from the given text."""
        text = text.strip()
        if not text.startswith(self.inline_tags[0]) or not text.endswith(self.inline_tags[1]):
            return

        return text[2:-2]

    def evaluate_block(self, template, context=None, escape=None, safe_wrapper=None):
        """Evaluate a template block."""
        raise NotImplementedError

    def evaluate_inline(self, expression, context=None, escape=None, safe_wrapper=None):
        """Evaluate an inline expression."""
        raise NotImplementedError
