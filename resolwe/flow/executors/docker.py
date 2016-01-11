"""Local workflow executor"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import random
import shlex
import subprocess

from django.conf import settings

from .local import FlowExecutor as LocalFlowExecutor


class FlowExecutor(LocalFlowExecutor):
    def start(self):
        container_image = settings.FLOW_EXECUTOR['CONTAINER_IMAGE']

        if self.data_id != 'no_data_id':
            container_name = 'resolwe_{}'.format(self.data_id)
        else:
            # set random container name for tests
            rand_int = random.randint(1000, 9999)
            container_name = 'resolwe_test_{}'.format(rand_int)

        self.proc = subprocess.Popen(
            shlex.split(
                'docker run --rm --interactive --name={} {} /bin/bash'.format(container_name, container_image)),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)

        self.stdout = self.proc.stdout

    def run_script(self, script):
        self.proc.stdin.write(os.linesep.join(['set -x', 'set +B', script]) + os.linesep)
        self.proc.stdin.close()

    def end(self):
        self.proc.wait()

        return self.proc.returncode

    def terminate(self, data_id):
        subprocess.call(shlex.split('docker rm -f {}').format(str(data_id)))
