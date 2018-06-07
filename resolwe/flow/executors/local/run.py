""".. Ignore pydocstyle D400.

.. autoclass:: resolwe.flow.executors.local.run.FlowExecutor

"""
import asyncio
import logging
import os
import shlex
from asyncio import subprocess

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

    async def start(self):
        """Start process execution."""
        # Workaround for pylint issue #1469
        # (https://github.com/PyCQA/pylint/issues/1469).
        self.proc = await subprocess.create_subprocess_exec(  # pylint: disable=no-member
            *shlex.split(self.command),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )

        self.stdout = self.proc.stdout

        return self.proc.pid

    async def run_script(self, script):
        """Execute the script and save results."""
        script = os.linesep.join(['set -x', 'set +B', script, 'exit']) + os.linesep
        self.proc.stdin.write(script.encode('utf-8'))
        await self.proc.stdin.drain()
        self.proc.stdin.close()

    async def end(self):
        """End process execution."""
        await self.proc.wait()

        return self.proc.returncode

    async def terminate(self):
        """Terminate a running script."""
        self.proc.terminate()

        await asyncio.wait_for(self.proc.wait(), self.kill_delay)
        if self.proc.returncode is None:
            self.proc.kill()
        await self.proc.wait()

        await super().terminate()
