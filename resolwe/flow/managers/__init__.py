"""Workflow workload managers."""
from .dispatcher import Manager

__all__ = ('manager')

manager = Manager()  # pylint: disable=invalid-name
