"""Workflow Expression Engines"""


class BaseExpressionEngine(object):

    """Represents a workflow expression engine."""

    def eval(self, data_id, script):
        """Evaluate the script and return executable."""
        raise NotImplementedError('subclasses of BaseExpressionEngine may require an eval() method')
