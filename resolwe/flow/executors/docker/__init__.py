"""Docker workflow executor."""
from __future__ import absolute_import, division, print_function, unicode_literals

import json
import os
import shlex
import subprocess
import tempfile

from django.conf import settings

from resolwe.flow.models import Process

from ..local import FlowExecutor as LocalFlowExecutor
from .seccomp import SECCOMP_POLICY


class FlowExecutor(LocalFlowExecutor):
    """Docker executor."""

    name = 'docker'

    def __init__(self, *args, **kwargs):
        """Initialize attributes."""
        super(FlowExecutor, self).__init__(*args, **kwargs)

        self.mappings_tools = None
        self.policy_file = None
        self.command = getattr(settings, 'FLOW_DOCKER_COMMAND', 'docker')

    def start(self):
        """Start process execution."""
        # arguments passed to the Docker command
        command_args = {
            'command': self.command,
            'container_image': self.requirements.get('image', settings.FLOW_EXECUTOR['CONTAINER_IMAGE']),
        }

        # Get limit defaults and overrides.
        limit_defaults = getattr(settings, 'FLOW_DOCKER_LIMIT_DEFAULTS', {})
        limit_overrides = getattr(settings, 'FLOW_DOCKER_LIMIT_OVERRIDES', {})

        # Set resource limits.
        limits = []
        if 'cores' in self.resources:
            # Each core is equivalent to 1024 CPU shares. The default for Docker containers
            # is 1024 shares (we don't need to explicitly set that).
            limits.append('--cpu-shares={}'.format(int(self.resources['cores']) * 1024))

        memory = limit_overrides.get('memory', {}).get(self.process.slug, None)
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
        if self.process.scheduling_class == Process.SCHEDULING_CLASS_INTERACTIVE:
            # TODO: This is not very good as each child gets the same limit.
            limits.append('--ulimit cpu={}'.format(limit_defaults.get('cpu_time_interactive', 30)))

        command_args['limits'] = ' '.join(limits)

        # set container name
        container_name_prefix = getattr(settings, 'FLOW_EXECUTOR', {}).get('CONTAINER_NAME_PREFIX', 'resolwe')
        command_args['container_name'] = '--name={}_{}'.format(container_name_prefix, self.data_id)

        if 'network' in self.resources:
            # Configure Docker network mode for the container (if specified).
            # By default, current Docker versions use the 'bridge' mode which
            # creates a network stack on the default Docker bridge.
            network = getattr(settings, 'FLOW_EXECUTOR', {}).get('NETWORK', '')
            command_args['network'] = '--net={}'.format(network) if network else ''
        else:
            # No network if not specified.
            command_args['network'] = '--net=none'

        # Security options.
        security = []

        # Generate and set seccomp policy to limit syscalls.
        self.policy_file = tempfile.NamedTemporaryFile(mode='w')
        json.dump(SECCOMP_POLICY, self.policy_file)
        self.policy_file.file.flush()
        if not getattr(settings, 'FLOW_DOCKER_DISABLE_SECCOMP', False):
            security.append('--security-opt seccomp={}'.format(self.policy_file.name))

        # Drop all capabilities and only add ones that are needed.
        security.append('--cap-drop=all')
        # TODO: We should not required dac_override, but we need to fix permissions first (e.g., use --user).
        security.append('--cap-add=dac_override')
        # TODO: We should not require setuid/setgid, but we need to get rid of gosu first (e.g., use --user).
        security.append('--cap-add=setuid')
        security.append('--cap-add=setgid')
        # TODO: We should not required chown, but we need to fix permissions first (e.g., use --user).
        security.append('--cap-add=chown')

        command_args['security'] = ' '.join(security)

        # render Docker mappings in FLOW_DOCKER_MAPPINGS setting
        mappings_template = getattr(settings, 'FLOW_DOCKER_MAPPINGS', [])
        context = {'data_id': self.data_id}
        mappings = [{key.format(**context): value.format(**context) for key, value in template.items()}
                    for template in mappings_template]

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

        # create environment variables to pass certain information to the
        # process running in the container
        command_args['envs'] = ' '.join(['--env={name}={value}'.format(**env) for env in [
            {'name': 'HOST_UID', 'value': os.getuid()},
            {'name': 'HOST_GID', 'value': os.getgid()},
        ]])

        # a login Bash shell is needed to source ~/.bash_profile
        command_args['shell'] = '/bin/bash --login'

        self.proc = subprocess.Popen(
            shlex.split(
                '{command} run --rm --interactive {container_name} {network} {volumes} {envs} {limits} '
                '{security} {workdir} {container_image} {shell}'.format(**command_args)),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            universal_newlines=True)

        self.stdout = self.proc.stdout

    def run_script(self, script):
        """Execute the script and save results."""
        mappings = getattr(settings, 'FLOW_DOCKER_MAPPINGS', {})
        for map_ in mappings:
            script = script.replace(map_['src'], map_['dest'])
        # create a Bash command to add all the tools to PATH
        tools_paths = ':'.join([map_["dest"] for map_ in self.mappings_tools])
        add_tools_path = 'export PATH=$PATH:{}'.format(tools_paths)
        # Spawn another child bash, to avoid running anything as PID 1, which has special
        # signal handling (e.g., cannot be SIGKILL-ed from inside).
        self.proc.stdin.write('/bin/bash --login; exit $?' + os.linesep)
        self.proc.stdin.write(os.linesep.join(['set -x', 'set +B', add_tools_path, script]) + os.linesep)
        self.proc.stdin.close()

    def end(self):
        """End process execution."""
        self.proc.wait()
        if self.policy_file is not None:
            self.policy_file.close()
            self.policy_file = None

        return self.proc.returncode

    def terminate(self, data_id):
        """Terminate a running script."""
        subprocess.call(shlex.split('{} rm -f {}'.format(self.command, data_id)))
