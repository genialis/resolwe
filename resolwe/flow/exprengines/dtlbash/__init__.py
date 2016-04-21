"""Django template language based expression engine."""
from django import template
from django.conf import settings

from resolwe.flow.exprengines import BaseExpressionEngine
from resolwe.flow.models import Data, hydrate_input_references, hydrate_input_uploads, render_template


class ExpressionEngine(BaseExpressionEngine):

    """Represents a workflow expression engine."""

    def eval(self, data):
        """Evaluate the script and return executable."""
        try:
            inputs = data.input.copy()
            hydrate_input_references(inputs, data.process.input_schema)
            hydrate_input_uploads(inputs, data.process.input_schema)

            inputs['proc'] = {
                'data_id': data.id,
                'data_path': settings.FLOW_EXECUTOR['DATA_PATH'],
            }

            script_template = data.process.run.get('bash', '')

            script = render_template(script_template, template.Context(inputs))

        except template.TemplateSyntaxError as ex:
            data.status = Data.STATUS_ERROR
            data.process_error.append('Error in process script: {}'.format(ex))
            data.save()
            return None

        return script
