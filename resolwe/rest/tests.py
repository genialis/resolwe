# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import itertools

import six

from rest_framework.test import APIRequestFactory, force_authenticate

from resolwe.flow.models import Data, Entity, Process
from resolwe.flow.views import EntityViewSet
from resolwe.test import TestCase

factory = APIRequestFactory()  # pylint: disable=invalid-name


class ProjectionTest(TestCase):
    def setUp(self):
        super(ProjectionTest, self).setUp()

        self.entity = Entity.objects.create(name="Test entity", contributor=self.contributor)
        process = Process.objects.create(
            name="Test process",
            contributor=self.contributor,
            output_schema=[
                {'name': 'foo', 'label': 'Foo', 'group': [
                    {'name': 'bar', 'label': 'Bar', 'type': 'basic:integer:'},
                    {'name': 'hello', 'label': 'Hello', 'type': 'basic:string:'},
                ]},
                {'name': 'another', 'label': 'Another', 'type': 'basic:integer:'},
            ]
        )
        data_output = {
            'foo': {
                'bar': 42,
                'hello': 'world',
            },
            'another': 3,
        }
        self.data_output = data_output
        self.data = Data.objects.create(name="Test data", contributor=self.contributor, process=process,
                                        output=data_output)
        self.data_2 = Data.objects.create(name="Test data 2", contributor=self.contributor, process=process,
                                          output=data_output)

        self.entity.data.add(self.data)
        self.entity.data.add(self.data_2)

        self.entity_viewset = EntityViewSet.as_view(actions={'get': 'list'})

    def get_projection(self, fields):
        request = factory.get('/', {'fields': ','.join(fields), 'hydrate_data': '1'}, format='json')
        force_authenticate(request, self.admin)
        return self.entity_viewset(request).data

    def test_projection(self):
        # Test top-level projection.
        all_fields = self.get_projection([])[0].keys()
        for field_count in range(1, 3):
            for fields in itertools.combinations(all_fields, field_count):
                data = self.get_projection(fields)[0]
                six.assertCountEqual(self, data.keys(), set(fields + ('permissions',)))

        # Test nested projection.
        data = self.get_projection(['data__name'])[0]
        six.assertCountEqual(self, data.keys(), ['data', 'permissions'])
        self.assertEqual(len(data['data']), 2)
        for item in data['data']:
            six.assertCountEqual(self, item.keys(), ['name'])

        # Test top-level JSON projection.
        data = self.get_projection(['data__output'])[0]
        six.assertCountEqual(self, data.keys(), ['data', 'permissions'])
        self.assertEqual(len(data['data']), 2)
        for item in data['data']:
            six.assertCountEqual(self, item.keys(), ['output'])
            self.assertEqual(item['output'], self.data_output)

        # Test nested projection into JSON.
        data = self.get_projection(['data__name', 'data__output__foo__bar'])[0]
        self.assertEqual(len(data['data']), 2)
        for item in data['data']:
            six.assertCountEqual(self, item.keys(), ['name', 'output'])
            six.assertCountEqual(self, item['output'].keys(), ['foo'])
            six.assertCountEqual(self, item['output']['foo'].keys(), ['bar'])
            self.assertEqual(item['output']['foo']['bar'], 42)
