""".. Ignore pydocstyle D400.

================
Abstract Manager
================

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import os

from django.conf import settings
from django.db import IntegrityError, transaction

from resolwe.flow.engine import InvalidEngineError, load_engines
from resolwe.flow.execution_engines import ExecutionError
from resolwe.flow.models import Data, Process
from resolwe.flow.utils import iterate_fields
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

            # None values are valid and should be ignored.
            if value is None:
                continue

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
        """Initialize arguments."""
        self.discover_engines()

    def discover_engines(self):
        """Discover configured engines."""
        executor = getattr(settings, 'FLOW_EXECUTOR', {}).get('NAME', 'resolwe.flow.executors.local')
        self.executor = self.load_executor(executor)
        expression_engines = getattr(settings, 'FLOW_EXPRESSION_ENGINES', ['resolwe.flow.expression_engines.jinja'])
        self.expression_engines = self.load_expression_engines(expression_engines)
        execution_engines = getattr(settings, 'FLOW_EXECUTION_ENGINES', ['resolwe.flow.execution_engines.bash'])
        self.execution_engines = self.load_execution_engines(execution_engines)

    def _include_environment_variables(self, program):
        """Define environment variables."""
        env_vars = {
            'RESOLWE_HOST_URL': getattr(settings, 'RESOLWE_HOST_URL', 'localhost'),
        }

        set_env = getattr(settings, 'FLOW_EXECUTOR', {}).get('SET_ENV', {})
        env_vars.update(set_env)

        # TODO: Use shlex.quote when py2 support dropped
        export_commands = ['export {}="{}"'.format(key, value.replace('"', '\"')) for key, value in env_vars.items()]
        return os.linesep.join(export_commands) + os.linesep + program

    def run(self, data_id, script, priority='normal', run_sync=False, verbosity=1):
        """Run process."""
        raise NotImplementedError('`run` function not implemented')

    def communicate(self, run_sync=False, verbosity=1):
        """Resolve task dependencies and run the task."""
        queue = []
        try:
            for data in Data.objects.filter(status=Data.STATUS_RESOLVING):
                with transaction.atomic():
                    # Lock for update. Note that we want this transaction to be as short as
                    # possible in order to reduce contention and avoid deadlocks. This is
                    # why we do not lock all resolving objects for update, but instead only
                    # lock one object at a time. This allows managers running in parallel
                    # to process different objects.
                    data = Data.objects.select_for_update().get(pk=data.pk)
                    if data.status != Data.STATUS_RESOLVING:
                        # The object might have already been processed while waiting for
                        # the lock to be obtained. In this case, skip the object.
                        continue

                    dep_status = dependency_status(data)

                    if dep_status == Data.STATUS_ERROR:
                        data.status = Data.STATUS_ERROR
                        data.process_error.append("One or more inputs have status ERROR")
                        data.process_rc = 1
                        data.save()
                        continue

                    elif dep_status != Data.STATUS_DONE:
                        continue

                    if data.process.run:
                        try:
                            execution_engine = data.process.run.get('language', None)
                            # Evaluation by the execution engine may spawn additional data objects
                            # and perform other queries on the database. Queries of all possible
                            # execution engines need to be audited for possibilities of deadlocks
                            # in case any additional locks are introduced. Currently, we only take
                            # an explicit lock on the currently processing object.
                            program = self.get_execution_engine(execution_engine).evaluate(data)
                        except (ExecutionError, InvalidEngineError) as error:
                            data.status = Data.STATUS_ERROR
                            data.process_error.append('Error in process script: {}'.format(error))
                            data.save()
                            continue
                    else:
                        # If there is no run section, then we should not try to run anything. But the
                        # program must not be set to None as then the process will be stuck in waiting state.
                        program = ''

                    if data.status != Data.STATUS_DONE:
                        # The data object may already be marked as done by the execution engine. In this
                        # case we must not revert the status to STATUS_WAITING.
                        data.status = Data.STATUS_WAITING
                    data.save(render_name=True)

                    if program is not None:
                        priority = 'normal'
                        if data.process.persistence == Process.PERSISTENCE_TEMP:
                            # TODO: This should probably be removed.
                            priority = 'high'
                        if data.process.scheduling_class == Process.SCHEDULING_CLASS_INTERACTIVE:
                            priority = 'high'

                        program = self._include_environment_variables(program)

                        queue.append((data.id, priority, program))

                    # All data objects created by the execution engine are commited after this
                    # point and may be processed by other managers running in parallel. At the
                    # same time, lock for the current data object is released.

        except IntegrityError as exp:
            logger.error(__("IntegrityError in manager {}", exp))
            return

        for data_id, priority, program in queue:
            if verbosity >= 1:
                print("Running", program)
            self.run(data_id, program, priority=priority, verbosity=verbosity)

    def get_executor(self):
        """Return an executor instance."""
        return self.executor

    def get_expression_engine(self, name):
        """Return an expression engine instance."""
        try:
            return self.expression_engines[name]
        except KeyError:
            raise InvalidEngineError('Unsupported expression engine: {}'.format(name))

    def get_execution_engine(self, name):
        """Return an execution engine instance."""
        try:
            return self.execution_engines[name]
        except KeyError:
            raise InvalidEngineError('Unsupported execution engine: {}'.format(name))

    def load_executor(self, executor_name):
        """Load process executor."""
        engines = load_engines(self, 'FlowExecutor', 'executors', [executor_name], 'EXECUTOR', 'executor')
        # List conversion needed for Python 3, where values() returns a view.
        return list(engines.values())[0]

    def load_expression_engines(self, engines):
        """Load expression engines."""
        return load_engines(self, 'ExpressionEngine', 'expression_engines', engines)

    def load_execution_engines(self, engines):
        """Load execution engines."""
        return load_engines(self, 'ExecutionEngine', 'execution_engines', engines)
