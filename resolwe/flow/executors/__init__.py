from __future__ import absolute_import, division, print_function, unicode_literals
"""Workflow Executors"""

import json
import logging
import os
import six

from django.apps import apps
from django.conf import settings

if settings.USE_TZ:
    from django.utils.timezone import now
else:
    import datetime  # pylint: disable=wrong-import-order
    now = datetime.datetime.now  # pylint: disable=invalid-name

from resolwe.flow.models import Data, dict_dot, Process
from resolwe.utils import BraceMessage as __


logger = logging.getLogger(__name__)  # pylint: disable=invalid-name
CWD = os.getcwd()


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

    """Represents a workflow executor

    .. WARNING::
        Ensure this class can be serialized since some engines will
        use it this way (e.g. Celery).
    """

    def get_tools(self):
        tools_paths = []
        for app_config in apps.get_app_configs():
            proc_path = os.path.join(app_config.path, 'tools')
            if os.path.isdir(proc_path):
                tools_paths.append(proc_path)
        return tools_paths

    def start(self):
        pass

    def run_script(self, script):
        raise NotImplementedError('`run_script` function must be defined')

    def end(self):
        pass

    def get_stdout(self):
        return self.stdout

    def update_data_status(self, **kwargs):
        data = Data.objects.get(pk=self.data_id)
        for key, value in kwargs.items():
            setattr(data, key, value)

        # Ensure that we only update the fields that were changed.
        data.save(update_fields=kwargs.keys())

    def run(self, data_id, script, verbosity=1):
        """Execute the script and save results."""
        if verbosity >= 1:
            print('RUN: {} {}'.format(data_id, script))

        self.data_id = data_id

        dir_mode = settings.FLOW_EXECUTOR.get('DATA_DIR_MODE', 0o755)

        output_path = os.path.join(settings.FLOW_EXECUTOR['DATA_PATH'], str(data_id))

        os.mkdir(output_path)
        # os.mkdir is not guaranteed to set the given mode
        os.chmod(output_path, dir_mode)
        os.chdir(output_path)

        log_file = open('stdout.txt', 'w+')
        json_file = open('jsonout.txt', 'w+')

        proc_pid = self.start()

        self.update_data_status(
            status=Data.STATUS_PROCESSING,
            started=now(),
            process_pid=proc_pid
        )

        # Run processor and handle intermediate results
        self.run_script(script)
        spawn_processors = []
        output = {}
        process_error, process_warning, process_info = [], [], []
        process_progress, process_rc = 0, 0

        # read processor output
        try:
            stdout = self.get_stdout()
            while True:
                line = stdout.readline()
                if not line:
                    break

                try:
                    if line.strip().startswith('run'):
                        # Save processor and spawn if no errors
                        log_file.write(line)
                        log_file.flush()

                        for obj in iterjson(line[3:].strip()):
                            spawn_processors.append(obj)
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
                                        updates['status'] = Data.STATUS_ERROR
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
                                            updates['status'] = Data.STATUS_ERROR
                                    elif key == 'proc.progress':
                                        process_progress = int(float(val) * 100)
                                        updates['process_progress'] = process_progress
                                else:
                                    dict_dot(output, key, val)
                                    updates['output'] = output

                        if updates:
                            updates['modified'] = now()
                            self.update_data_status(**updates)

                        if process_rc > 0:
                            log_file.close()
                            json_file.close()
                            os.chdir(CWD)
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
            logger.error(__("Out of memory: {}", ex))

        except IOError as ex:
            # TODO: if ex.errno == 28: no more free space
            raise ex
        finally:
            # Store results
            log_file.close()
            json_file.close()
            os.chdir(CWD)

        return_code = self.end()

        if process_rc < return_code:
            process_rc = return_code

        if process_rc == 0:
            self.update_data_status(
                status=Data.STATUS_DONE,
                process_progress=100,
                finished=now()
            )
        else:
            self.update_data_status(
                status=Data.STATUS_ERROR,
                process_progress=100,
                process_rc=process_rc,
                finished=now()
            )

        # try:
        #     # Cleanup after processor
        #     data_purge(data_ids=[data_id], delete=True, verbosity=0)
        # except:  # pylint: disable=bare-except
        #     logger.error(__("Purge error:\n\n{}", traceback.format_exc()))

        # if not update_data(data):  # Data was deleted
        #     # Restore original directory
        #     os.chdir(settings.PROJECT_ROOT)
        #     return

        if spawn_processors and Data.objects.get(pk=self.data_id).status == Data.STATUS_DONE:
            # Spawn processors
            for d in spawn_processors:
                d['contributor'] = Data.objects.get(pk=self.data_id).contributor
                d['process'] = Process.objects.get(slug=d['process'])
                Data.objects.create(**d)

        # Restore original directory
        # os.chdir(settings.PROJECT_ROOT)

        # return data_id

    def terminate(self, data_id):
        """Terminate a running script."""
        raise NotImplementedError('subclasses of BaseFlowExecutor may require a terminate() method')
