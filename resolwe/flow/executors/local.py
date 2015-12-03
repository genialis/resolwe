"""Local workflow executor"""
from __future__ import absolute_import, division, print_function, unicode_literals

import datetime
import json
import logging
import os
import subprocess
import time

from django.conf import settings

from resolwe.flow.executors import BaseFlowExecutor
from resolwe.flow.models import Data, dict_dot
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


class FlowExecutor(BaseFlowExecutor):

    """Local dataflow executor proxy."""

    def __init__(self):
        self.processes = {}
        self.kill_delay = 5

    def run(self, data_id, script):
        print('RUN: {} {}'.format(data_id, script))

        output_path = os.path.join(settings.FLOW['EXECUTOR']['DATA_PATH'], str(data_id))
        os.mkdir(output_path, 0o775)
        os.chdir(output_path)

        log_file = open(os.path.join(output_path, 'stdout.txt'), 'w+')
        json_file = open(os.path.join(output_path, 'jsonout.txt'), 'w+')

        proc = subprocess.Popen(['/bin/bash'],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, universal_newlines=True)

        self.processes[data_id] = proc

        Data.objects.filter(id=data_id).update(
            status=Data.STATUS_PROCESSING,
            started=datetime.datetime.utcnow(),
            process_pid=proc.pid)

        # Run processor and handle intermediate results
        proc.stdin.write(os.linesep.join(['set -x', 'set +B', script, 'exit']) + os.linesep)
        proc.stdin.close()
        spawn_processors = []
        output = {}
        process_error, process_warning, process_info = [], [], []
        process_progress, process_rc = 0, 0

        # read processor output
        try:
            while True:
                line = proc.stdout.readline()
                print(line)
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
                            for key, val in obj.iteritems():
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
                                        updates['status'] = Data.STATUS_ERROR
                                    elif key == 'proc.progress':
                                        process_progress = int(float(val) * 100)
                                        updates['process_progress'] = process_progress
                                else:
                                    dict_dot(output, key, val)
                                    updates['output'] = output

                        if updates:
                            updates['modified'] = datetime.datetime.utcnow()
                            Data.objects.filter(id=data_id).update(**updates)

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

        proc.wait()

        if process_rc < proc.returncode:
            process_rc = proc.returncode

        if process_rc == 0:
            Data.objects.filter(id=data_id).update(
                status=Data.STATUS_DONE,
                process_progress=100,
                finished=datetime.datetime.utcnow())
        else:
            Data.objects.filter(id=data_id).update(
                status=Data.STATUS_ERROR,
                process_progress=100,
                process_rc=process_rc,
                finished=datetime.datetime.utcnow())

        # try:
        #     # Cleanup after processor
        #     data_purge(data_ids=[data_id], delete=True, verbosity=0)
        # except:  # pylint: disable=bare-except
        #     logger.error(__("Purge error:\n\n{}", traceback.format_exc()))

        # if not update_data(data):  # Data was deleted
        #     # Restore original directory
        #     os.chdir(settings.PROJECT_ROOT)
        #     return

        # if data.status == Data.STATUS_DONE and spawn_processors:
        #     # Spawn processors
        #     os.chdir(HOME)
        #     for d in spawn_processors:
        #         d['case_ids'] = [case_id]
        #         d['author_id'] = data.author_id
        #         data = Data(**d)
        #         data.save()

        #         run_triggers(data_id=data.id, autorun=True)

        # Restore original directory
        # os.chdir(settings.PROJECT_ROOT)

        # return data_id

    def terminate(self, data_id):
        proc = self.processes[data_id]
        proc.terminate()

        time.sleep(self.kill_delay)
        if proc.poll() is None:
            proc.kill()
