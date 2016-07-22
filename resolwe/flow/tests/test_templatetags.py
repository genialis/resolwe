# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import shutil

from django import template
from django.conf import settings
from django.contrib.auth import get_user_model
from django.test import TestCase

from resolwe.flow.managers import manager
from resolwe.flow.models import Process, Data


class ProcessFieldsTagsTest(TestCase):
    def setUp(self):
        User = get_user_model()
        self.contributor = User.objects.create_user('test_user', 'test_pwd')

    def tearDown(self):
        for data in Data.objects.all():
            data_dir = os.path.join(settings.FLOW_EXECUTOR['DATA_DIR'], str(data.id))
            shutil.rmtree(data_dir, ignore_errors=True)

    def test_templatetags(self):
        input_process = Process.objects.create(
            name='Input process',
            contributor=self.contributor,
            type='data:test:inputobject:',
        )
        input_data = Data.objects.create(
            name='Input Data object',
            contributor=self.contributor,
            process=input_process,
        )

        process = Process.objects.create(
            name='Test template tags',
            contributor=self.contributor,
            type='test:data:templatetags:',
            input_schema=[
                {'name': 'input_data', 'type': 'data:test:inputobject:'},
            ],
            output_schema=[
                {'name': 'name', 'type': 'basic:string:'},
                {'name': 'id', 'type': 'basic:integer:'},
                {'name': 'type', 'type': 'basic:string:'},
            ],
            run={
                'bash': """
re-save name "{{ input_data | name }}"
re-save id {{ input_data | id }}
re-save type {{ input_data | type }}
"""
            }

        )
        data = Data.objects.create(
            name='Data object',
            contributor=self.contributor,
            process=process,
            input={'input_data': input_data.pk},
        )

        manager.communicate(verbosity=0)

        # update output
        data = Data.objects.get(pk=data.pk)

        self.assertEqual(data.output['name'], input_data.name)
        self.assertEqual(data.output['id'], input_data.pk)
        self.assertEqual(data.output['type'], input_process.type)
