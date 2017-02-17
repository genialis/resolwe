# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from django.test import override_settings

from resolwe.flow.managers import manager
from resolwe.flow.models import Data, Process
from resolwe.test import TestCase


class EnvVarsTest(TestCase):

    @override_settings(RESOLWE_HOST_URL='some.special.host')
    def test_envvars(self):
        process = Process.objects.create(
            name='Test environment variables',
            requirements={'expression-engine': 'jinja'},
            contributor=self.contributor,
            type='test:data:envvars:',
            input_schema=[],
            output_schema=[
                {'name': 'resolweapihost', 'type': 'basic:string:'},
            ],
            run={
                'language': 'bash',
                'program': """
re-save resolweapihost $RESOLWE_HOST_URL
"""
            }
        )

        data = Data.objects.create(
            name='Data object',
            contributor=self.contributor,
            process=process,
            input={},
        )

        manager.communicate(verbosity=0)

        # update output
        data = Data.objects.get(pk=data.pk)

        self.assertEqual(data.output['resolweapihost'], 'some.special.host')
