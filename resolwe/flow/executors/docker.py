"""Local workflow executor"""
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import random
import shlex
import subprocess

from django.conf import settings

from .local import FlowExecutor as LocalFlowExecutor


class FlowExecutor(LocalFlowExecutor):

    def __init__(self, *args, **kwargs):
        super(FlowExecutor, self).__init__(*args, **kwargs)
        self.command = settings.FLOW_EXECUTOR.get('COMMAND', 'docker')

    def start(self):
        # arguments passed to the Docker command
        command_args = {
            'command': self.command,
            'container_image': settings.FLOW_EXECUTOR['CONTAINER_IMAGE'],
        }

        if self.data_id != 'no_data_id':
            data_id = self.data_id
        else:
            # set random container name for tests
            data_id = 'test_{}'.format(random.randint(1000, 9999))
        command_args['container_name'] = '--name=resolwe_{}'.format(data_id)

        # render Docker mappings in FLOW_DOCKER_MAPPINGS setting
        mappings_template = getattr(settings, 'FLOW_DOCKER_MAPPINGS', [])
        context = {'data_id': self.data_id}
        mappings = [{key.format(**context): value.format(**context) for key, value in template.items()}
                    for template in mappings_template]

        # create mappings for tools
        # NOTE: To prevent processes tampering with tools, all tools are mounted read-only
        self.mappings_tools = [{'src': tool, 'dest': '/usr/local/bin/resolwe/{}'.format(i), 'mode': 'ro'}
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
                '{command} run --rm --interactive {container_name} {volumes} '
                '{envs} {workdir} {container_image} {shell}'.format(**command_args)),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            universal_newlines=True)

        self.stdout = self.proc.stdout

    def run_script(self, script):
        mappings = getattr(settings, 'FLOW_DOCKER_MAPPINGS', {})
        for map_ in mappings:
            script = script.replace(map_['src'], map_['dest'])
        # create a Bash command to add all the tools to PATH
        tools_paths = ':'.join([map_["dest"] for map_ in self.mappings_tools])
        add_tools_path = 'export PATH=$PATH:{}'.format(tools_paths)
        self.proc.stdin.write(os.linesep.join(['set -x', 'set +B', add_tools_path, script]) + os.linesep)
        self.proc.stdin.close()

    def end(self):
        self.proc.wait()

        return self.proc.returncode

    def terminate(self, data_id):
        subprocess.call(shlex.split('{} rm -f {}'.format(self.command, data_id)))
