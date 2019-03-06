""".. Ignore pydocstyle D400.

.. autoclass:: resolwe.flow.executors.docker.run.FlowExecutor
    :members:

"""
# pylint: disable=logging-format-interpolation
import asyncio
import json
import logging
import os
import shlex
import tempfile
import time
from asyncio import subprocess

from ..global_settings import DATA_LOCATION, PROCESS_META, SETTINGS
from ..local.run import FlowExecutor as LocalFlowExecutor
from ..protocol import ExecutorFiles
from . import constants
from .seccomp import SECCOMP_POLICY

DOCKER_START_TIMEOUT = 60

# Limits of containers' access to memory. We set the limit to ensure
# processes are stable and do not get killed by OOM signal.
DOCKER_MEMORY_HARD_LIMIT_BUFFER = 100
DOCKER_MEMORY_SWAP_RATIO = 2
DOCKER_MEMORY_SWAPPINESS = 1

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class FlowExecutor(LocalFlowExecutor):
    """Docker executor."""

    name = 'docker'

    def __init__(self, *args, **kwargs):
        """Initialize attributes."""
        super().__init__(*args, **kwargs)

        self.container_name_prefix = None
        self.tools_volumes = None
        self.temporary_files = []
        self.command = SETTINGS.get('FLOW_DOCKER_COMMAND', 'docker')

    def _generate_container_name(self):
        """Generate unique container name."""
        return '{}_{}'.format(self.container_name_prefix, self.data_id)

    async def start(self):
        """Start process execution."""
        # arguments passed to the Docker command
        command_args = {
            'command': self.command,
            'container_image': self.requirements.get('image', constants.DEFAULT_CONTAINER_IMAGE),
        }

        # Get limit defaults.
        limit_defaults = SETTINGS.get('FLOW_PROCESS_RESOURCE_DEFAULTS', {})

        # Set resource limits.
        limits = []
        # Each core is equivalent to 1024 CPU shares. The default for Docker containers
        # is 1024 shares (we don't need to explicitly set that).
        limits.append('--cpu-shares={}'.format(int(self.process['resource_limits']['cores']) * 1024))

        # Some SWAP is needed to avoid OOM signal. Swappiness is low to prevent
        # extensive usage of SWAP (this would reduce the performance).
        memory = self.process['resource_limits']['memory'] + DOCKER_MEMORY_HARD_LIMIT_BUFFER
        memory_swap = int(memory * DOCKER_MEMORY_SWAP_RATIO)

        limits.append('--memory={}m'.format(memory))
        limits.append('--memory-swap={}m'.format(memory_swap))
        limits.append('--memory-reservation={}m'.format(self.process['resource_limits']['memory']))
        limits.append('--memory-swappiness={}'.format(DOCKER_MEMORY_SWAPPINESS))

        # Set ulimits for interactive processes to prevent them from running too long.
        if self.process['scheduling_class'] == PROCESS_META['SCHEDULING_CLASS_INTERACTIVE']:
            # TODO: This is not very good as each child gets the same limit.
            limits.append('--ulimit cpu={}'.format(limit_defaults.get('cpu_time_interactive', 30)))

        command_args['limits'] = ' '.join(limits)

        # set container name
        self.container_name_prefix = SETTINGS.get('FLOW_EXECUTOR', {}).get('CONTAINER_NAME_PREFIX', 'resolwe')
        command_args['container_name'] = '--name={}'.format(self._generate_container_name())

        if 'network' in self.resources:
            # Configure Docker network mode for the container (if specified).
            # By default, current Docker versions use the 'bridge' mode which
            # creates a network stack on the default Docker bridge.
            network = SETTINGS.get('FLOW_EXECUTOR', {}).get('NETWORK', '')
            command_args['network'] = '--net={}'.format(network) if network else ''
        else:
            # No network if not specified.
            command_args['network'] = '--net=none'

        # Security options.
        security = []

        # Generate and set seccomp policy to limit syscalls.
        policy_file = tempfile.NamedTemporaryFile(mode='w')
        json.dump(SECCOMP_POLICY, policy_file)
        policy_file.file.flush()
        if not SETTINGS.get('FLOW_DOCKER_DISABLE_SECCOMP', False):
            security.append('--security-opt seccomp={}'.format(policy_file.name))
        self.temporary_files.append(policy_file)

        # Drop all capabilities and only add ones that are needed.
        security.append('--cap-drop=all')

        command_args['security'] = ' '.join(security)

        # Setup Docker volumes.
        def new_volume(kind, base_dir_name, volume, path=None, read_only=True):
            """Generate a new volume entry.

            :param kind: Kind of volume, which is used for getting extra options from
                settings (the ``FLOW_DOCKER_VOLUME_EXTRA_OPTIONS`` setting)
            :param base_dir_name: Name of base directory setting for volume source path
            :param volume: Destination volume mount point
            :param path: Optional additional path atoms appended to source path
            :param read_only: True to make the volume read-only
            """
            if path is None:
                path = []

            path = [str(atom) for atom in path]

            options = set(SETTINGS.get('FLOW_DOCKER_VOLUME_EXTRA_OPTIONS', {}).get(kind, '').split(','))
            options.discard('')
            # Do not allow modification of read-only option.
            options.discard('ro')
            options.discard('rw')

            if read_only:
                options.add('ro')
            else:
                options.add('rw')

            return {
                'src': os.path.join(SETTINGS['FLOW_EXECUTOR'].get(base_dir_name, ''), *path),
                'dest': volume,
                'options': ','.join(options),
            }

        volumes = [
            new_volume(
                'data', 'DATA_DIR', constants.DATA_VOLUME, [DATA_LOCATION['subpath']], read_only=False
            ),
            new_volume('data_all', 'DATA_DIR', constants.DATA_ALL_VOLUME),
            new_volume('upload', 'UPLOAD_DIR', constants.UPLOAD_VOLUME, read_only=False),
            new_volume(
                'secrets',
                'RUNTIME_DIR',
                constants.SECRETS_VOLUME,
                [DATA_LOCATION['subpath'], ExecutorFiles.SECRETS_DIR]
            ),
        ]

        # Generate dummy passwd and create mappings for it. This is required because some tools
        # inside the container may try to lookup the given UID/GID and will crash if they don't
        # exist. So we create minimal user/group files.
        passwd_file = tempfile.NamedTemporaryFile(mode='w')
        passwd_file.write('root:x:0:0:root:/root:/bin/bash\n')
        passwd_file.write('user:x:{}:{}:user:/:/bin/bash\n'.format(os.getuid(), os.getgid()))
        passwd_file.file.flush()
        self.temporary_files.append(passwd_file)

        group_file = tempfile.NamedTemporaryFile(mode='w')
        group_file.write('root:x:0:\n')
        group_file.write('user:x:{}:user\n'.format(os.getgid()))
        group_file.file.flush()
        self.temporary_files.append(group_file)

        volumes += [
            new_volume('users', None, '/etc/passwd', [passwd_file.name]),
            new_volume('users', None, '/etc/group', [group_file.name]),
        ]

        # Create volumes for tools.
        # NOTE: To prevent processes tampering with tools, all tools are mounted read-only
        self.tools_volumes = []
        for index, tool in enumerate(self.get_tools_paths()):
            self.tools_volumes.append(new_volume(
                'tools',
                None,
                os.path.join('/usr/local/bin/resolwe', str(index)),
                [tool]
            ))

        volumes += self.tools_volumes

        # Create volumes for runtime (all read-only).
        runtime_volume_maps = SETTINGS.get('RUNTIME_VOLUME_MAPS', None)
        if runtime_volume_maps:
            for src, dst in runtime_volume_maps.items():
                volumes.append(new_volume(
                    'runtime',
                    'RUNTIME_DIR',
                    dst,
                    [DATA_LOCATION['subpath'], src],
                ))

        # Add any extra volumes verbatim.
        volumes += SETTINGS.get('FLOW_DOCKER_EXTRA_VOLUMES', [])

        # Make sure that tmp dir exists.
        os.makedirs(constants.TMPDIR, mode=0o755, exist_ok=True)

        # Create Docker --volume parameters from volumes.
        command_args['volumes'] = ' '.join(['--volume="{src}":"{dest}":{options}'.format(**volume)
                                            for volume in volumes])

        # Set working directory to the data volume.
        command_args['workdir'] = '--workdir={}'.format(constants.DATA_VOLUME)

        # Change user inside the container.
        command_args['user'] = '--user={}:{}'.format(os.getuid(), os.getgid())

        # A non-login Bash shell should be used here (a subshell will be spawned later).
        command_args['shell'] = '/bin/bash'

        # Check if image exists locally. If not, command will exit with non-zero returncode
        check_command = '{command} image inspect {container_image}'.format(**command_args)

        logger.debug("Checking existence of docker image: {}".format(command_args['container_image']))

        check_proc = await subprocess.create_subprocess_exec(  # pylint: disable=no-member
            *shlex.split(check_command),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        await check_proc.communicate()

        if check_proc.returncode != 0:
            pull_command = '{command} pull {container_image}'.format(**command_args)

            logger.info("Pulling docker image: {}".format(command_args['container_image']))

            pull_proc = await subprocess.create_subprocess_exec(  # pylint: disable=no-member
                *shlex.split(pull_command),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            _, stderr = await pull_proc.communicate()

            if pull_proc.returncode != 0:
                error_msg = "Docker failed to pull {} image.".format(command_args['container_image'])
                if stderr:
                    error_msg = '\n'.join([error_msg, stderr.decode('utf-8')])
                raise RuntimeError(error_msg)

        docker_command = (
            '{command} run --rm --interactive {container_name} {network} {volumes} {limits} '
            '{security} {workdir} {user} {container_image} {shell}'.format(**command_args)
        )

        logger.info("Starting docker container with command: {}".format(docker_command))
        start_time = time.time()

        # Workaround for pylint issue #1469
        # (https://github.com/PyCQA/pylint/issues/1469).
        self.proc = await subprocess.create_subprocess_exec(  # pylint: disable=no-member
            *shlex.split(docker_command),
            limit=4 * (2 ** 20),  # 4MB buffer size for line buffering
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT
        )

        stdout = []

        async def wait_for_container():
            """Wait for Docker container to start to avoid blocking the code that uses it."""
            self.proc.stdin.write(('echo PING' + os.linesep).encode('utf-8'))
            await self.proc.stdin.drain()
            while True:
                line = await self.proc.stdout.readline()
                stdout.append(line)
                if line.rstrip() == b'PING':
                    break
                if self.proc.stdout.at_eof():
                    raise RuntimeError()

        try:
            await asyncio.wait_for(wait_for_container(), timeout=DOCKER_START_TIMEOUT)
        except (asyncio.TimeoutError, RuntimeError):
            error_msg = "Docker container has not started for {} seconds.".format(DOCKER_START_TIMEOUT)
            stdout = ''.join([line.decode('utf-8') for line in stdout if line])
            if stdout:
                error_msg = '\n'.join([error_msg, stdout])
            raise RuntimeError(error_msg)

        end_time = time.time()
        logger.info("It took {:.2f}s for Docker container to start".format(end_time - start_time))

        self.stdout = self.proc.stdout

    async def run_script(self, script):
        """Execute the script and save results."""
        # Create a Bash command to add all the tools to PATH.
        tools_paths = ':'.join([map_["dest"] for map_ in self.tools_volumes])
        add_tools_path = 'export PATH=$PATH:{}'.format(tools_paths)
        # Spawn another child bash, to avoid running anything as PID 1, which has special
        # signal handling (e.g., cannot be SIGKILL-ed from inside).
        # A login Bash shell is needed to source /etc/profile.
        bash_line = '/bin/bash --login; exit $?' + os.linesep
        script = os.linesep.join(['set -x', 'set +B', add_tools_path, script]) + os.linesep
        self.proc.stdin.write(bash_line.encode('utf-8'))
        await self.proc.stdin.drain()
        self.proc.stdin.write(script.encode('utf-8'))
        await self.proc.stdin.drain()
        self.proc.stdin.close()

    async def end(self):
        """End process execution."""
        try:
            await self.proc.wait()
        finally:
            # Cleanup temporary files.
            for temporary_file in self.temporary_files:
                temporary_file.close()
            self.temporary_files = []

        return self.proc.returncode

    async def terminate(self):
        """Terminate a running script."""
        # Workaround for pylint issue #1469
        # (https://github.com/PyCQA/pylint/issues/1469).
        cmd = await subprocess.create_subprocess_exec(  # pylint: disable=no-member
            *shlex.split('{} rm -f {}'.format(self.command, self._generate_container_name()))
        )
        await cmd.wait()
        await self.proc.wait()

        await super().terminate()
