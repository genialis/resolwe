""".. Ignore pydocstyle D400.

==============
Flow Executors
==============

.. autoclass:: resolwe.flow.executors.run.BaseFlowExecutor
    :members:

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import datetime
import json
import logging
import os
import shutil
import traceback
import uuid
from collections import defaultdict

# NOTE: If the imports here are changed, the executors' requirements.txt
# file must also be updated accordingly.
import redis
import six

from .protocol import ExecutorFiles, ExecutorProtocol  # pylint: disable=import-error

DESERIALIZED_FILES = {}  # pylint: disable=invalid-name
with open(ExecutorFiles.EXECUTOR_SETTINGS, 'rt') as _settings_file:
    DESERIALIZED_FILES[ExecutorFiles.EXECUTOR_SETTINGS] = json.load(_settings_file)
    for _file_name in DESERIALIZED_FILES[ExecutorFiles.EXECUTOR_SETTINGS][ExecutorFiles.FILE_LIST_KEY]:
        with open(_file_name, 'rt') as _json_file:
            DESERIALIZED_FILES[_file_name] = json.load(_json_file)

EXECUTOR_SETTINGS = DESERIALIZED_FILES[ExecutorFiles.EXECUTOR_SETTINGS]
SETTINGS = DESERIALIZED_FILES[ExecutorFiles.DJANGO_SETTINGS]
DATA = DESERIALIZED_FILES[ExecutorFiles.DATA]
DATA_META = DESERIALIZED_FILES[ExecutorFiles.DATA_META]
PROCESS = DESERIALIZED_FILES[ExecutorFiles.PROCESS]
PROCESS_META = DESERIALIZED_FILES[ExecutorFiles.PROCESS_META]

_REDIS_RETRIES = 60


now = datetime.datetime.now  # pylint: disable=invalid-name


logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def iterjson(text):
    """Decode JSON stream."""
    decoder = json.JSONDecoder()
    while len(text) > 0:
        obj, ndx = decoder.raw_decode(text)

        if not isinstance(obj, dict):
            raise ValueError()

        text = text[ndx:].lstrip('\r\n')
        yield obj


class BaseFlowExecutor(object):
    """Represents a workflow executor."""

    exported_files_mapper = defaultdict(dict)

    def __init__(self, *args, **kwargs):
        """Initialize attributes."""
        self.data_id = None
        self.process = None
        self.requirements = {}
        self.resources = {}
        # The Redis connection instance used to communicate with the manager listener.
        self.redis = redis.StrictRedis(**SETTINGS['FLOW_EXECUTOR'].get('REDIS_CONNECTION', {}))
        # This channel name will be used for all listener communication; Data object-specific.
        self.queue_response_channel = '{}.{}'.format(EXECUTOR_SETTINGS['REDIS_CHANNEL_PAIR'][1], DATA['id'])

    def get_tools(self):
        """Get tools paths."""
        tools_paths = SETTINGS['FLOW_EXECUTOR_TOOLS_PATHS']
        return tools_paths

    def start(self):
        """Start process execution."""
        pass

    def run_script(self, script):
        """Run process script."""
        raise NotImplementedError("Subclasses of BaseFlowExecutor must implement a run_script() method.")

    def end(self):
        """End process execution."""
        pass

    def get_stdout(self):
        """Get process' standard output."""
        return self.stdout  # pylint: disable=no-member

    def _send_manager_command(self, cmd, expect_reply=True, extra_fields={}):
        """Send a properly formatted command to the manager.

        :param cmd: The command to send (:class:`str`).
        :param expect_reply: If ``True``, wait for the manager to reply
            with an acknowledgement packet.
        :param extra_fields: A dictionary of extra information that's
            merged into the packet body (i.e. not under an extra key).
        """
        packet = {
            ExecutorProtocol.DATA_ID: DATA['id'],
            ExecutorProtocol.COMMAND: cmd,
        }
        packet.update(extra_fields)

        # TODO what happens here if the push fails? we don't have any realistic recourse,
        # so just let it explode and stop processing
        queue_channel = EXECUTOR_SETTINGS['REDIS_CHANNEL_PAIR'][0]
        try:
            self.redis.rpush(queue_channel, json.dumps(packet))
        except Exception:  # pylint: disable=broad-except
            logger.error("Error sending command to manager:\n\n%s", traceback.format_exc())
            raise

        if not expect_reply:
            return

        while True:
            for _ in range(_REDIS_RETRIES):
                response = self.redis.blpop(self.queue_response_channel, timeout=1)
                if response:
                    break
            else:
                # NOTE: If there's still no response after a few seconds, the system is broken
                # enough that it makes sense to give up; we're isolated here, so if the manager
                # doesn't respond, we can't really do much more than just crash
                raise RuntimeError("No response from the manager after {} retries.".format(_REDIS_RETRIES))

            _, item = response
            packet = json.loads(item.decode('utf-8'))
            assert packet[ExecutorProtocol.RESULT] == ExecutorProtocol.RESULT_OK
            break

    def update_data_status(self, **kwargs):
        """Update (PATCH) Data object.

        :param kwargs: The dictionary of
            :class:`~resolwe.flow.models.Data` attributes to be changed.
        """
        self._send_manager_command(ExecutorProtocol.UPDATE, extra_fields={
            ExecutorProtocol.UPDATE_CHANGESET: kwargs
        })

    def run(self, data_id, script, verbosity=1):
        """Execute the script and save results."""
        if verbosity >= 1:
            print('RUN: {} {}'.format(data_id, script))

        self.data_id = data_id

        # Fetch data instance to get any executor requirements.
        self.process = PROCESS
        requirements = self.process['requirements']
        self.requirements = requirements.get('executor', {}).get(self.name, {})  # pylint: disable=no-member
        self.resources = requirements.get('resources', {})

        os.chdir(EXECUTOR_SETTINGS['DATA_DIR'])
        log_file = open('stdout.txt', 'w+')
        json_file = open('jsonout.txt', 'w+')

        proc_pid = self.start()

        self.update_data_status(
            status=DATA_META['STATUS_PROCESSING'],
            started=now().isoformat(),
            process_pid=proc_pid
        )

        # Run process and handle intermediate results
        self.run_script(script)
        spawn_processes = []
        output = {}
        process_error, process_warning, process_info = [], [], []
        process_progress, process_rc = 0, 0

        # read process output
        try:
            stdout = self.get_stdout()
            while True:
                line = stdout.readline()
                if not line:
                    break

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
                            for key, val in six.iteritems(obj):
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
                            updates['modified'] = now().isoformat()
                            self.update_data_status(**updates)

                        if process_rc > 0:
                            log_file.close()
                            json_file.close()
                            self._send_manager_command(ExecutorProtocol.FINISH, extra_fields={
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
            logger.error("Out of memory: %s", ex)

        except IOError as ex:
            # TODO: if ex.errno == 28: no more free space
            raise ex
        finally:
            # Store results
            stdout.close()
            log_file.close()
            json_file.close()

        return_code = self.end()

        if process_rc < return_code:
            process_rc = return_code

        # send a notification to the executor listener that we're done
        finish_fields = {
            ExecutorProtocol.FINISH_PROCESS_RC: process_rc
        }
        if spawn_processes and process_rc == 0:
            finish_fields[ExecutorProtocol.FINISH_SPAWN_PROCESSES] = spawn_processes
            finish_fields[ExecutorProtocol.FINISH_EXPORTED_FILES] = self.exported_files_mapper
        self._send_manager_command(ExecutorProtocol.FINISH, extra_fields=finish_fields)
        # the feedback key deletes itself once the list is drained

    def terminate(self, data_id):
        """Terminate a running script."""
        raise NotImplementedError("Subclasses of BaseFlowExecutor must implement a terminate() method.")
