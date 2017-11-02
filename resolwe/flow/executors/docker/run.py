"""Docker workflow executor."""
from __future__ import absolute_import, division, print_function, unicode_literals

import json
import os
import shlex
import subprocess
import tempfile

from ..local.run import FlowExecutor as LocalFlowExecutor
from ..run import PROCESS_META, SETTINGS
from .seccomp import SECCOMP_POLICY


class FlowExecutor(LocalFlowExecutor):
    """Docker executor."""

    name = 'docker'

    def __init__(self, *args, **kwargs):
        """Initialize attributes."""
        super(FlowExecutor, self).__init__(*args, **kwargs)

        self.container_name_prefix = None
        self.mappings_tools = None
        self.temporary_files = []
        self.command = SETTINGS.get('FLOW_DOCKER_COMMAND', 'docker')

    def start(self):
        """Start process execution."""
        # arguments passed to the Docker command
        command_args = {
            'command': self.command,
            'container_image': self.requirements.get('image', SETTINGS['FLOW_EXECUTOR']['CONTAINER_IMAGE']),
        }

        # Get limit defaults and overrides.
        limit_defaults = SETTINGS.get('FLOW_DOCKER_LIMIT_DEFAULTS', {})
        limit_overrides = SETTINGS.get('FLOW_DOCKER_LIMIT_OVERRIDES', {})

        # Set resource limits.
        limits = []
        if 'cores' in self.resources:
            # Each core is equivalent to 1024 CPU shares. The default for Docker containers
            # is 1024 shares (we don't need to explicitly set that).
            limits.append('--cpu-shares={}'.format(int(self.resources['cores']) * 1024))

        memory = limit_overrides.get('memory', {}).get(self.process['slug'], None)
        if memory is None:
            memory = int(self.resources.get(
                'memory',
                # If no memory resource is configured, check settings.
                limit_defaults.get('memory', 4096)
            ))

        # Set both memory and swap limits as we want to enforce the total amount of memory
        # used (otherwise swap would not count against the limit).
        limits.append('--memory={0}m --memory-swap={0}m'.format(memory))

        # Set ulimits for interactive processes to prevent them from running too long.
        if self.process['scheduling_class'] == PROCESS_META['SCHEDULING_CLASS_INTERACTIVE']:
            # TODO: This is not very good as each child gets the same limit.
            limits.append('--ulimit cpu={}'.format(limit_defaults.get('cpu_time_interactive', 30)))

        command_args['limits'] = ' '.join(limits)

        # set container name
        self.container_name_prefix = SETTINGS.get('FLOW_EXECUTOR', {}).get('CONTAINER_NAME_PREFIX', 'resolwe')
        command_args['container_name'] = '--name={}_{}'.format(self.container_name_prefix, self.data_id)

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

        # render Docker mappings in FLOW_DOCKER_MAPPINGS setting
        mappings_template = SETTINGS.get('FLOW_DOCKER_MAPPINGS', [])
        context = {'data_id': self.data_id}
        mappings = [{key.format(**context): value.format(**context) for key, value in template.items()}
                    for template in mappings_template]

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

        mappings.append({'src': passwd_file.name, 'dest': '/etc/passwd', 'mode': 'ro,Z'})
        mappings.append({'src': group_file.name, 'dest': '/etc/group', 'mode': 'ro,Z'})

        # create mappings for tools
        # NOTE: To prevent processes tampering with tools, all tools are mounted read-only
        # NOTE: Since the tools are shared among all containers they must use the shared SELinux
        # label (z option)
        self.mappings_tools = [{'src': tool, 'dest': '/usr/local/bin/resolwe/{}'.format(i), 'mode': 'ro,z'}
                               for i, tool in enumerate(self.get_tools())]
        mappings += self.mappings_tools
        # create Docker --volume parameters from mappings
        command_args['volumes'] = ' '.join(['--volume="{src}":"{dest}":{mode}'.format(**map_)
                                            for map_ in mappings])

        # set working directory inside the container to the mapped directory of
        # the current Data's directory
        command_args['workdir'] = ''
        for template in mappings_template:
            if '{data_id}' in template['src']:
                command_args['workdir'] = '--workdir={}'.format(template['dest'])

        # Change user inside the container.
        command_args['user'] = '--user={}:{}'.format(os.getuid(), os.getgid())

        # A non-login Bash shell should be used here (a subshell will be spawned later).
        command_args['shell'] = '/bin/bash'

        self.proc = subprocess.Popen(
            shlex.split(
                '{command} run --rm --interactive {container_name} {network} {volumes} {limits} '
                '{security} {workdir} {user} {container_image} {shell}'.format(**command_args)),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            universal_newlines=True)

        self.stdout = self.proc.stdout

    def run_script(self, script):
        """Execute the script and save results."""
        mappings = SETTINGS.get('FLOW_DOCKER_MAPPINGS', {})
        for map_ in mappings:
            script = script.replace(map_['src'], map_['dest'])
        # create a Bash command to add all the tools to PATH
        tools_paths = ':'.join([map_["dest"] for map_ in self.mappings_tools])
        add_tools_path = 'export PATH=$PATH:{}'.format(tools_paths)
        # Spawn another child bash, to avoid running anything as PID 1, which has special
        # signal handling (e.g., cannot be SIGKILL-ed from inside).
        # A login Bash shell is needed to source /etc/profile.
        self.proc.stdin.write('/bin/bash --login; exit $?' + os.linesep)
        self.proc.stdin.write(os.linesep.join(['set -x', 'set +B', add_tools_path, script]) + os.linesep)
        self.proc.stdin.close()

    def end(self):
        """End process execution."""
        try:
            self.proc.wait()
        finally:
            # Cleanup temporary files.
            for temporary_file in self.temporary_files:
                temporary_file.close()
            self.temporary_files = []

        return self.proc.returncode

    def terminate(self, data_id):
        """Terminate a running script."""
        subprocess.call(shlex.split('{} rm -f {}_{}'.format(self.command, self.container_name_prefix, self.data_id)))
