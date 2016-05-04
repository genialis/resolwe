"""Local workflow executor"""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import os
import shlex
import subprocess
import time

from django.conf import settings

from resolwe.flow.executors import BaseFlowExecutor


logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class FlowExecutor(BaseFlowExecutor):

    """Local dataflow executor proxy."""

    def __init__(self, *args, **kwargs):
        super(FlowExecutor, self).__init__(*args, **kwargs)
        self.command = settings.FLOW_EXECUTOR.get('COMMAND', '/bin/bash')
        self.processes = {}
        self.kill_delay = 5

    def start(self):
        self.proc = subprocess.Popen(shlex.split(self.command),
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, universal_newlines=True)

        self.processes[self.data_id] = self.proc
        self.stdout = self.proc.stdout

        return self.proc.pid

    def run_script(self, script):
        self.proc.stdin.write(os.linesep.join(['set -x', 'set +B', script, 'exit']) + os.linesep)
        self.proc.stdin.close()

    def end(self):
        self.proc.wait()

        return self.proc.returncode

    def terminate(self, data_id):
        proc = self.processes[data_id]
        proc.terminate()

        time.sleep(self.kill_delay)
        if proc.poll() is None:
            proc.kill()
