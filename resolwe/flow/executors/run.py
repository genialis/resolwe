""".. Ignore pydocstyle D400.

==========
Base Class
==========

.. autoclass:: resolwe.flow.executors.run.BaseFlowExecutor
    :members:

"""
# pylint: disable=logging-format-interpolation
import asyncio
import json
import logging
import os
import shutil
import signal
import sys
import uuid
from collections import defaultdict

from .global_settings import DATA_META, EXECUTOR_SETTINGS, PROCESS, SETTINGS
from .manager_commands import send_manager_command
from .protocol import ExecutorProtocol

# NOTE: If the imports here are changed, the executors' requirements.txt
# file must also be updated accordingly.

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def iterjson(text):
    """Decode JSON stream."""
    decoder = json.JSONDecoder()
    while text:
        obj, ndx = decoder.raw_decode(text)

        if not isinstance(obj, dict):
            raise ValueError()

        text = text[ndx:].lstrip('\r\n')
        yield obj


class BaseFlowExecutor:
    """Represents a workflow executor."""

    exported_files_mapper = defaultdict(dict)

    def __init__(self, *args, **kwargs):
        """Initialize attributes."""
        self.data_id = None
        self.process = None
        self.requirements = {}
        self.resources = {}

        asyncio.get_event_loop().add_signal_handler(
            signal.SIGTERM, lambda: asyncio.ensure_future(self._exit_gracefully())
        )

    async def _exit_gracefully(self):
        """Handle SIGTERM signal."""
        await self.update_data_status(
            process_error=["Executor was killed by the scheduling system."],
            status=DATA_META['STATUS_ERROR'],
        )

        await self.terminate()

    async def _send_manager_command(self, *args, **kwargs):
        """Send an update to manager and terminate the process if it fails."""
        resp = await send_manager_command(*args, **kwargs)

        if resp is False:
            await self.terminate()

    def get_tools_paths(self):
        """Get tools paths."""
        tools_paths = SETTINGS['FLOW_EXECUTOR_TOOLS_PATHS']
        return tools_paths

    async def start(self):
        """Start process execution."""
        pass

    async def run_script(self, script):
        """Run process script."""
        raise NotImplementedError("Subclasses of BaseFlowExecutor must implement a run_script() method.")

    async def end(self):
        """End process execution."""
        pass

    def get_stdout(self):
        """Get process' standard output."""
        return self.stdout  # pylint: disable=no-member

    async def update_data_status(self, **kwargs):
        """Update (PATCH) Data object.

        :param kwargs: The dictionary of
            :class:`~resolwe.flow.models.Data` attributes to be changed.
        """
        await self._send_manager_command(ExecutorProtocol.UPDATE, extra_fields={
            ExecutorProtocol.UPDATE_CHANGESET: kwargs
        })

    async def run(self, data_id, script):
        """Execute the script and save results."""
        logger.debug("Executor for Data with id {} has started.".format(data_id))
        try:
            finish_fields = await self._run(data_id, script)
        except SystemExit:
            raise
        except Exception as error:  # pylint: disable=broad-except
            logger.exception("Unhandled exception in executor")

            # Send error report.
            await self.update_data_status(process_error=[str(error)], status=DATA_META['STATUS_ERROR'])

            finish_fields = {
                ExecutorProtocol.FINISH_PROCESS_RC: 1,
            }

        if finish_fields is not None:
            await self._send_manager_command(ExecutorProtocol.FINISH, extra_fields=finish_fields)

        # The response channel (Redis list) is deleted automatically once the list is drained, so
        # there is no need to remove it manually.

    def _create_file(self, filename):
        """Ensure a new file is created and opened for writing."""
        file_descriptor = os.open(filename, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
        return os.fdopen(file_descriptor, 'w')

    async def _run(self, data_id, script):
        """Execute the script and save results."""
        self.data_id = data_id

        # Fetch data instance to get any executor requirements.
        self.process = PROCESS
        requirements = self.process['requirements']
        self.requirements = requirements.get('executor', {}).get(self.name, {})  # pylint: disable=no-member
        self.resources = requirements.get('resources', {})

        logger.debug("Preparing output files for Data with id {}".format(data_id))
        os.chdir(EXECUTOR_SETTINGS['DATA_DIR'])
        try:
            log_file = self._create_file('stdout.txt')
            json_file = self._create_file('jsonout.txt')
        except FileExistsError:
            logger.error("Stdout or jsonout out file already exists.")
            # Looks like executor was already ran for this Data object,
            # so don't raise the error to prevent setting status to error.
            await self._send_manager_command(ExecutorProtocol.ABORT, expect_reply=False)
            return

        proc_pid = await self.start()

        await self.update_data_status(
            status=DATA_META['STATUS_PROCESSING'],
            process_pid=proc_pid
        )

        # Run process and handle intermediate results
        logger.info("Running program for Data with id {}".format(data_id))
        logger.debug("The program for Data with id {} is: \n{}".format(data_id, script))
        await self.run_script(script)
        spawn_processes = []
        output = {}
        process_error, process_warning, process_info = [], [], []
        process_progress, process_rc = 0, 0

        # read process output
        try:
            stdout = self.get_stdout()
            while True:
                line = await stdout.readline()
                logger.debug("Process's output: {}".format(line.strip()))

                if not line:
                    break
                line = line.decode('utf-8')

                try:
                    if line.strip().startswith('run'):
                        # Save process and spawn if no errors
                        log_file.write(line)
                        log_file.flush()

                        for obj in iterjson(line[3:].strip()):
                            spawn_processes.append(obj)
                    elif line.strip().startswith('export'):
                        file_name = line[6:].strip()

                        export_folder = SETTINGS['FLOW_EXECUTOR']['UPLOAD_DIR']
                        unique_name = 'export_{}'.format(uuid.uuid4().hex)
                        export_path = os.path.join(export_folder, unique_name)

                        self.exported_files_mapper[self.data_id][file_name] = unique_name

                        shutil.move(file_name, export_path)
                    else:
                        # If JSON, save to MongoDB
                        updates = {}
                        for obj in iterjson(line):
                            for key, val in obj.items():
                                if key.startswith('proc.'):
                                    if key == 'proc.error':
                                        process_error.append(val)
                                        if not process_rc:
                                            process_rc = 1
                                            updates['process_rc'] = process_rc
                                        updates['process_error'] = process_error
                                        updates['status'] = DATA_META['STATUS_ERROR']
                                    elif key == 'proc.warning':
                                        process_warning.append(val)
                                        updates['process_warning'] = process_warning
                                    elif key == 'proc.info':
                                        process_info.append(val)
                                        updates['process_info'] = process_info
                                    elif key == 'proc.rc':
                                        process_rc = int(val)
                                        updates['process_rc'] = process_rc
                                        if process_rc != 0:
                                            updates['status'] = DATA_META['STATUS_ERROR']
                                    elif key == 'proc.progress':
                                        process_progress = int(float(val) * 100)
                                        updates['process_progress'] = process_progress
                                else:
                                    output[key] = val
                                    updates['output'] = output

                        if updates:
                            await self.update_data_status(**updates)
                            # Process meta fields are collected in listener, so we can clear them.
                            process_error, process_warning, process_info = [], [], []

                        if process_rc > 0:
                            log_file.close()
                            json_file.close()
                            await self._send_manager_command(ExecutorProtocol.FINISH, extra_fields={
                                ExecutorProtocol.FINISH_PROCESS_RC: process_rc
                            })
                            return

                        # Debug output
                        # Not referenced in Data object
                        json_file.write(line)
                        json_file.flush()

                except ValueError as ex:
                    # Ignore if not JSON
                    log_file.write(line)
                    log_file.flush()

        except MemoryError as ex:
            logger.error("Out of memory:\n\n{}".format(ex))

        except IOError as ex:
            # TODO: if ex.errno == 28: no more free space
            raise ex
        finally:
            # Store results
            log_file.close()
            json_file.close()

        return_code = await self.end()

        if process_rc < return_code:
            process_rc = return_code

        # send a notification to the executor listener that we're done
        finish_fields = {
            ExecutorProtocol.FINISH_PROCESS_RC: process_rc
        }
        if spawn_processes and process_rc == 0:
            finish_fields[ExecutorProtocol.FINISH_SPAWN_PROCESSES] = spawn_processes
            finish_fields[ExecutorProtocol.FINISH_EXPORTED_FILES] = self.exported_files_mapper

        return finish_fields

    async def terminate(self):
        """Terminate a running script."""
        sys.exit(1)
