"""An execution engine that outputs bash programs."""
import copy

import shellescape

from django.conf import settings

from resolwe.flow.execution_engines.base import BaseExecutionEngine
from resolwe.flow.execution_engines.exceptions import ExecutionError
from resolwe.flow.expression_engines import EvaluationError
from resolwe.flow.models.utils import hydrate_input_references, hydrate_input_uploads


class SafeString(str):
    """String wrapper for marking strings safe."""

    pass


class ExecutionEngine(BaseExecutionEngine):
    """An execution engine that outputs bash programs."""

    name = 'bash'

    def evaluate(self, data):
        """Evaluate the code needed to compute a given Data object."""
        try:
            inputs = copy.deepcopy(data.input)
            hydrate_input_references(inputs, data.process.input_schema)
            hydrate_input_uploads(inputs, data.process.input_schema)

            # Include special 'proc' variable in the context.
            inputs['proc'] = {
                'data_id': data.id,
                'data_dir': self.manager.get_executor().resolve_data_path(),
            }

            # Include special 'requirements' variable in the context.
            inputs['requirements'] = data.process.requirements
            # Inject default values and change resources according to
            # the current Django configuration.
            inputs['requirements']['resources'] = data.process.get_resource_limits()

            script_template = data.process.run.get('program', '')

            # Get the appropriate expression engine. If none is defined, do not evaluate
            # any expressions.
            expression_engine = data.process.requirements.get('expression-engine', None)
            if not expression_engine:
                return script_template

            return self.get_expression_engine(expression_engine).evaluate_block(
                script_template, inputs,
                escape=self._escape,
                safe_wrapper=SafeString,
            )
        except EvaluationError as error:
            raise ExecutionError('{}'.format(error))

    def _escape(self, value):
        """Escape given value unless it is safe."""
        if isinstance(value, SafeString):
            return value

        return shellescape.quote(value)
