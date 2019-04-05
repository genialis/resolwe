"""An execution engine that supports workflow specifications."""
import collections

import yaml

from django.db import transaction

from resolwe.flow.execution_engines.base import BaseExecutionEngine
from resolwe.flow.execution_engines.exceptions import ExecutionError
from resolwe.flow.expression_engines import EvaluationError
from resolwe.flow.models import Data, DataDependency, Process
from resolwe.permissions.utils import copy_permissions


class ExecutionEngine(BaseExecutionEngine):
    """An execution engine that supports workflow specifications."""

    name = 'workflow'

    def discover_process(self, path):
        """Perform process discovery in given path.

        This method will be called during process registration and
        should return a list of dictionaries with discovered process
        schemas.
        """
        if not path.lower().endswith(('.yml', '.yaml')):
            return []

        with open(path) as fn:
            schemas = yaml.load(fn, Loader=yaml.FullLoader)
        if not schemas:
            # TODO: Logger.
            # self.stderr.write("Could not read YAML file {}".format(schema_file))
            return []

        process_schemas = []
        for schema in schemas:
            if 'run' not in schema:
                continue

            # NOTE: This currently assumes that 'bash' is the default.
            if schema['run'].get('language', 'bash') != 'workflow':
                continue

            process_schemas.append(schema)

        return process_schemas

    def get_output_schema(self, process):
        """Return any additional output schema for the process."""
        return [
            {'name': 'steps', 'label': "Steps", 'type': 'list:data:'},
        ]

    def _evaluate_expressions(self, expression_engine, step_id, values, context):
        """Recursively evaluate expressions in a dictionary of values."""
        if expression_engine is None:
            return values

        processed = {}
        for name, value in values.items():
            if isinstance(value, str):
                value = value.strip()
                try:
                    expression = expression_engine.get_inline_expression(value)
                    if expression is not None:
                        # Inline expression.
                        value = expression_engine.evaluate_inline(expression, context)
                    else:
                        # Block expression.
                        value = expression_engine.evaluate_block(value, context)
                except EvaluationError as error:
                    raise ExecutionError('Error while evaluating expression for step "{}":\n{}'.format(
                        step_id, error
                    ))
            elif isinstance(value, dict):
                value = self._evaluate_expressions(expression_engine, step_id, value, context)

            processed[name] = value

        return processed

    @transaction.atomic
    def evaluate(self, data):
        """Evaluate the code needed to compute a given Data object."""
        expression_engine = data.process.requirements.get('expression-engine', None)
        if expression_engine is not None:
            expression_engine = self.get_expression_engine(expression_engine)

        # Parse steps.
        steps = data.process.run.get('program', None)
        if steps is None:
            return

        if not isinstance(steps, list):
            raise ExecutionError('Workflow program must be a list of steps.')

        # Expression engine evaluation context.
        context = {
            'input': data.input,
            'steps': collections.OrderedDict(),
        }

        for index, step in enumerate(steps):
            try:
                step_id = step['id']
                step_slug = step['run']
            except KeyError as error:
                raise ExecutionError('Incorrect definition of step "{}", missing property "{}".'.format(
                    step.get('id', index), error
                ))

            # Fetch target process.
            process = Process.objects.filter(slug=step_slug).order_by('-version').first()
            if not process:
                raise ExecutionError('Incorrect definition of step "{}", invalid process "{}".'.format(
                    step_id, step_slug
                ))

            # Process all input variables.
            step_input = step.get('input', {})
            if not isinstance(step_input, dict):
                raise ExecutionError('Incorrect definition of step "{}", input must be a dictionary.'.format(
                    step_id
                ))

            data_input = self._evaluate_expressions(expression_engine, step_id, step_input, context)

            # Create the data object.
            data_object = Data.objects.create(
                process=process,
                contributor=data.contributor,
                tags=data.tags,
                input=data_input,
            )
            DataDependency.objects.create(
                parent=data,
                child=data_object,
                kind=DataDependency.KIND_SUBPROCESS,
            )

            # Copy permissions.
            copy_permissions(data, data_object)

            # Copy collections.
            for collection in data.collection_set.all():
                collection.data.add(data_object)

            context['steps'][step_id] = data_object.pk

        # Immediately set our status to done and output all data object identifiers.
        data.output = {
            'steps': list(context['steps'].values()),
        }
        data.status = Data.STATUS_DONE
