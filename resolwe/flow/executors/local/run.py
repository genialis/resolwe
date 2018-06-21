""".. Ignore pydocstyle D400.

.. autoclass:: resolwe.flow.executors.local.run.FlowExecutor

"""
import logging
import os
import shlex
import subprocess
import time

from ..run import BaseFlowExecutor

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class FlowExecutor(BaseFlowExecutor):
    """Local dataflow executor proxy."""

    name = 'local'

    def __init__(self, *args, **kwargs):
        """Initialize attributes."""
        super().__init__(*args, **kwargs)

        self.kill_delay = 5
        self.proc = None
        self.stdout = None
        self.command = '/bin/bash'

    def start(self):
        """Start process execution."""
        self.proc = subprocess.Popen(shlex.split(self.command),
                                     stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, universal_newlines=True)

        self.stdout = self.proc.stdout

        return self.proc.pid

    def run_script(self, script):
        """Execute the script and save results."""
        self.proc.stdin.write(os.linesep.join(['set -x', 'set +B', script, 'exit']) + os.linesep)
        self.proc.stdin.close()

    def end(self):
        """End process execution."""
        self.proc.wait()

        return self.proc.returncode

    def terminate(self):
        """Terminate a running script."""
        self.proc.terminate()

        time.sleep(self.kill_delay)
        if self.proc.poll() is None:
            self.proc.kill()

        super().terminate()
