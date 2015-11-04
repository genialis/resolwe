from django import template

from resolwe.flow.exprengines import BaseExpressionEngine
from resolwe.flow.models import Data, hydrate_input_references


class ExpressionEngine(BaseExpressionEngine):

    """Represents a workflow expression engine."""

    def eval(self, data):
        """Evaluate the script and return executable."""
        try:
            script_template = data.process.run.get('script', '')
            inputs = data.input.copy()
            hydrate_input_references(inputs, data.process.input_schema)
            # hydrate_input_uploads(inputs, data.process.input_schema)

            info = {}
            info['data_id'] = data.id
            # info['case_ids'] = data.case_ids
            # info['data_path'] = settings.RUNTIME['data_path']
            # info['slugs_path'] = settings.RUNTIME['slugs_path']
            inputs['proc'] = info  # add script info

            script = template.Template(
                '{% load resource_filters %}{% load mathfilters %}' + script_template
            ).render(template.Context(inputs))

        except template.TemplateSyntaxError as ex:
            data.status = Data.STATUS_ERROR
            data.process_error.append('Error in process script: {}'.format(ex))
            data.save()
            return None

        return script
