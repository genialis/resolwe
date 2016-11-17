# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from django.contrib.auth import get_user_model
from django.test import TestCase

from resolwe.flow.models import Data, DescriptorSchema, Process


class DescriptorTestCase(TestCase):
    def setUp(self):
        user_model = get_user_model()
        self.user = user_model.objects.create(username='test_user')

        self.process = Process.objects.create(name="Dummy process", contributor=self.user)

        self.descriptor_schema = DescriptorSchema.objects.create(
            name='Descriptor schema',
            contributor=self.user,
            schema=[
                {'name': 'test_field', 'type': 'basic:string:', 'default': 'default value'}
            ]
        )

    def test_default_values(self):
        data = Data.objects.create(
            name='Data object',
            contributor=self.user,
            process=self.process,
            descriptor_schema=self.descriptor_schema,
        )
        self.assertEqual(data.descriptor['test_field'], 'default value')

        data = Data.objects.create(
            name='Data object 2',
            contributor=self.user,
            process=self.process,
            descriptor_schema=self.descriptor_schema,
            descriptor={'test_field': 'changed value'}
        )
        self.assertEqual(data.descriptor['test_field'], 'changed value')
