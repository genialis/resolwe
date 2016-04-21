from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import os
import pkgutil

from importlib import import_module

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db import IntegrityError, transaction
from django.utils._os import upath

from resolwe.flow.models import Data, iterate_fields
from resolwe.utils import BraceMessage as __


logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def dependency_status(data):
    """Return abstracted satus of dependencies.

    STATUS_ERROR .. one dependency has error status
    STATUS_DONE .. all dependencies have done status
    None .. other

    """
    for field_schema, fields in iterate_fields(data.input, data.process.input_schema):
        if (field_schema['type'].lower().startswith('data:') or
                field_schema['type'].lower().startswith('list:data:')):
            name = field_schema['name']
            value = fields[name]

            if field_schema['type'].lower().startswith('data:'):
                value = [value]

            for uid in value:
                try:
                    _data = Data.objects.get(id=uid)
                except Data.DoesNotExist:
                    return Data.STATUS_ERROR

                if _data.status == Data.STATUS_ERROR:
                    return Data.STATUS_ERROR

                if _data.status != Data.STATUS_DONE:
                    return None

    return Data.STATUS_DONE


class BaseManager(object):

    """Manager handles process job execution."""

    def __init__(self):
        self.executor = self.load_executor(settings.FLOW_EXECUTOR['NAME']).FlowExecutor()
        self.exprengines = self.load_exprengines(settings.FLOW_EXPRESSION_ENGINES)

    def run(self, data_id, script, run_sync=False, verbosity=1):
        raise NotImplementedError('`run` function not implemented')

    def communicate(self, run_sync=False, verbosity=1):
        """Resolving task dependancy and execution."""
        queue = []
        try:
            with transaction.atomic():
                for data in Data.objects.select_for_update().filter(status=Data.STATUS_RESOLVING):

                    dep_status = dependency_status(data)

                    if dep_status == Data.STATUS_ERROR:
                        data.status = Data.STATUS_ERROR
                        data.process_error.append("One or more inputs have status ERROR")
                        data.process_rc = 1
                        data.save()
                        continue

                    elif dep_status != Data.STATUS_DONE:
                        data.status = Data.STATUS_RESOLVING
                        data.save()
                        continue

                    script = self.exprengines['dtlbash'].eval(data)
                    if script is None:
                        continue

                    data.status = Data.STATUS_WAITING
                    data.save(render_name=True)

                    queue.append((data.id, script))

        except IntegrityError as exp:
            logger.error(__("IntegrityError in manager {}", exp))
            return

        for data_id, script in queue:
            if verbosity >= 1:
                print("Running", script)
            self.run(data_id, script, verbosity=verbosity)

    def load_executor(self, executor_name):
        try:
            return import_module('{}'.format(executor_name))
        except ImportError as ex:
            # The executor wasn't found. Display a helpful error message
            # listing all possible (built-in) executors.
            executor_dir = os.path.join(os.path.dirname(upath(__file__)), 'executors')

            try:
                builtin_executors = [name for _, name, _ in pkgutil.iter_modules([executor_dir])]
            except EnvironmentError:
                builtin_executors = []
            if executor_name not in ['resolwe.flow.executors.{}'.format(b) for b in builtin_executors]:
                executor_reprs = map(repr, sorted(builtin_executors))
                error_msg = ("{} isn't an available dataflow executors.\n"
                             "Try using 'resolwe.flow.executors.XXX', where XXX is one of:\n"
                             "    {}\n"
                             "Error was: {}".format(executor_name, ", ".join(executor_reprs), ex))
                raise ImproperlyConfigured(error_msg)
            else:
                # If there's some other error, this must be an error in Django
                raise

    def load_exprengines(self, exprengine_list):
        exprengines = {}

        for exprengine_name in exprengine_list:
            try:
                exprengine_module = import_module('{}'.format(exprengine_name))
                exprengine_key = exprengine_name.split('.')[-1]

                if exprengine_key in exprengines:
                    raise ImproperlyConfigured("Duplicated expression engine {}".format(exprengine_key))

                exprengines[exprengine_key] = exprengine_module.ExpressionEngine()

            except ImportError as ex:
                # The expression engine wasn't found. Display a helpful error message
                # listing all possible (built-in) expression engines.
                exprengine_dir = os.path.join(os.path.dirname(upath(__file__)), 'exprengines')

                try:
                    builtin_exprengines = [name for _, name, _ in pkgutil.iter_modules([exprengine_dir])]
                except EnvironmentError:
                    builtin_exprengines = []
                if exprengine_name not in ['resolwe.flow.exprengines.{}'.format(b) for b in builtin_exprengines]:
                    exprengine_reprs = map(repr, sorted(builtin_exprengines))
                    error_msg = ("{} isn't an available dataflow expression engine.\n"
                                 "Try using 'resolwe.flow.exprengines.XXX', where XXX is one of:\n"
                                 "    {}\n"
                                 "Error was: {}".format(exprengine_name, ", ".join(exprengine_reprs), ex))
                    raise ImproperlyConfigured(error_msg)
                else:
                    # If there's some other error, this must be an error in Django
                    raise

        return exprengines
