import logging
from importlib import import_module
import pkgutil

from django import template
from django.conf import settings
from django.db import IntegrityError, transaction

from resolwe.flow.models import Data, iterate_fields, hydrate_input_references
from resolwe.utils import BraceMessage as __


__all__ = ['manager']

logger = logging.getLogger(__name__)


def dependency_status(data):
    """Return abstracted satus of dependencies.

    STATUS_ERROR .. one dependency has error status
    STATUS_DONE .. all dependencies have done status
    None .. other

    """
    for field_schema, fields in iterate_fields(data.input, data.tool.input_schema):
        if (field_schema['type'].lower().startswith('data:') or
                field_schema['type'].lower().startswith('list:data:')):
            name = field_schema['name']
            value = fields[name]

            if field_schema['type'].lower().startswith('data:'):
                value = [value]

            for uid in value:
                try:
                    _data = Data.objects.get(id=uid)
                except mongoengine.DoesNotExist:
                    return Data.STATUS_ERROR

                if _data.status == Data.STATUS_ERROR:
                    return Data.STATUS_ERROR

                if _data.status != Data.STATUS_DONE:
                    return None

    return Data.STATUS_DONE


class Manager(object):

    def __init__(self):
        self.backend = self.load_backend(settings.FLOW['BACKEND']).FlowBackend()

    def communicate(self, run_sync=False, verbosity=1):
        """Resolving task dependancy and execution."""
        queue = []
        try:
            with transaction.atomic():
                for data in Data.objects.select_for_update().filter(status=Data.STATUS_RESOLVING):

                    dep_status = dependency_status(data)

                    if dep_status == Data.STATUS_ERROR:
                        data.status = Data.STATUS_ERROR
                        data.tool_error.append("One or more inputs have status ERROR")
                        data.tool_rc = 1
                        data.save()
                        continue

                    elif dep_status != Data.STATUS_DONE:
                        data.status = Data.STATUS_RESOLVING
                        data.save()
                        continue

                    data.status = Data.STATUS_WAITING
                    data.save()

                    script_template = data.tool.adapter
                    inputs = data.input.copy()
                    hydrate_input_references(inputs, data.tool.input_schema)
                    # hydrate_input_uploads(inputs, data.tool.input_schema)

                    info = {}
                    # info['case_ids'] = data.case_ids
                    info['data_id'] = data.id
                    # info['data_path'] = settings.RUNTIME['data_path']
                    # info['slugs_path'] = settings.RUNTIME['slugs_path']
                    inputs['proc'] = info  # add script info

                    script = template.Template('{% load resource_filters %}{% load mathfilters %}' +
                                               script_template).render(template.Context(inputs))

                    queue.append((data.id, script))

        except IntegrityError as exp:
            logger.error(__("IntegrityError in manager {}", exp.message))
            return

        for data_id, script in queue:
            self.backend.run(data_id, script)

    def load_backend(self, backend_name):
        """Look for a fully qualified workflow backend name."""
        try:
            return import_module('%s' % backend_name)
        except ImportError as e_user:
            # The database backend wasn't found. Display a helpful error message
            # listing all possible (built-in) database backends.
            backend_dir = os.path.join(os.path.dirname(upath(__file__)), 'backends')
            try:
                builtin_backends = [
                    name for _, name, ispkg in pkgutil.iter_modules([backend_dir])
                    if ispkg and name != 'dummy']
            except EnvironmentError:
                builtin_backends = []
            if backend_name not in ['resolwe.flow.backends.%s' % b for b in
                                    builtin_backends]:
                backend_reprs = map(repr, sorted(builtin_backends))
                error_msg = ("%r isn't an available dataflow backend.\n"
                             "Try using 'resolwe.flow.backends.XXX', where XXX "
                             "is one of:\n    %s\nError was: %s" %
                             (backend_name, ", ".join(backend_reprs), e_user))
                raise ImproperlyConfigured(error_msg)
            else:
                # If there's some other error, this must be an error in Django
                raise


manager = Manager()
