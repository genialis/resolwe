# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import shutil

from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import override_settings, TestCase

from resolwe.flow.managers import manager
from resolwe.flow.models import Process, Data


class EnvVarsTest(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.contributor = user_model.objects.create_user('test_user', 'test_pwd')

    def tearDown(self):
        for data in Data.objects.all():
            data_dir = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(data.id))
            shutil.rmtree(data_dir, ignore_errors=True)

    @override_settings(RESOLWE_API_HOST='some.special.host')
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
re-save resolweapihost $RESOLWE_API_HOST
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
