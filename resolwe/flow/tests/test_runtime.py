# pylint: disable=missing-docstring
from unittest import mock

from resolwe.flow.execution_engines.bash import ExecutionEngine, ExecutionError
from resolwe.flow.expression_engines.jinja import ExpressionEngine
from resolwe.test import TestCase


class ExecutionEngineTestCase(TestCase):
    def test_invalid_template(self):
        manager = mock.MagicMock()
        manager.get_expression_engine.return_value = ExpressionEngine(manager)

        data_mock = mock.MagicMock(process_error=[])
        data_mock.process.requirements = {
            "expression-engine": "jinja",
        }
        data_mock.process.run = {
            "language": "bash",
            "program": '{% if reads.type.startswith("data:reads:") %}',
        }

        with self.assertRaises(ExecutionError):
            ExecutionEngine(manager).evaluate(data_mock)

        # If no expression engine is passed, then there should be no error.
        data_mock.process.requirements = {}
        result = ExecutionEngine(manager).evaluate(data_mock)
        self.assertEqual(result, data_mock.process.run["program"])


class CeleryEngineTestCase(TestCase):
    def test_passed_to_celery(self):
        pass
