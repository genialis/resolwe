# pylint: disable=missing-docstring
from collections import OrderedDict

from guardian.shortcuts import assign_perm
from rest_framework.test import APIRequestFactory

from resolwe.flow.models import Collection, Data, DescriptorSchema, Entity, Process
from resolwe.flow.serializers import CollectionSerializer, DataSerializer, EntitySerializer
from resolwe.test import TestCase


class CollectionSerializerTest(TestCase):
    def setUp(self):
        super().setUp()

        process = Process.objects.create(
            name='Test process',
            contributor=self.contributor,
        )

        self.collection = Collection.objects.create(
            name='Test collection',
            contributor=self.contributor,
        )
        # In collection, user has `view` permission
        self.data_1 = Data.objects.create(
            name='First data',
            contributor=self.contributor,
            process=process,
        )
        # In collection, user has no permissions
        self.data_2 = Data.objects.create(
            name='Second data',
            contributor=self.contributor,
            process=process,
        )
        # Not in collection, user has `view` permission
        self.data_3 = Data.objects.create(
            name='Third data',
            contributor=self.contributor,
            process=process,
        )

        self.collection.data.add(self.data_1, self.data_2)

        assign_perm('view_collection', self.user, self.collection)
        assign_perm('view_data', self.user, self.data_1)
        assign_perm('view_data', self.user, self.data_3)

        self.factory = APIRequestFactory()

    def test_filter_data_permissions(self):
        request = self.factory.get('/')
        request.user = self.user
        request.query_params = {}

        serializer = CollectionSerializer(self.collection, context={'request': request})
        self.assertEqual(serializer.data['data'], [self.data_1.pk])


class EntitySerializerTest(TestCase):
    def setUp(self):
        super().setUp()

        process = Process.objects.create(
            name='Test process',
            contributor=self.contributor,
        )

        self.entity = Entity.objects.create(
            name='Test entity',
            contributor=self.contributor,
        )
        # In entity, user has `view` permission
        self.data_1 = Data.objects.create(
            name='First data',
            contributor=self.contributor,
            process=process,
        )
        # In entity, user has no permissions
        self.data_2 = Data.objects.create(
            name='Second data',
            contributor=self.contributor,
            process=process,
        )
        # Not in entity, user has `view` permission
        self.data_3 = Data.objects.create(
            name='Third data',
            contributor=self.contributor,
            process=process,
        )

        self.entity.data.add(self.data_1, self.data_2)

        assign_perm('view_entity', self.user, self.entity)
        assign_perm('view_data', self.user, self.data_1)
        assign_perm('view_data', self.user, self.data_3)

        self.factory = APIRequestFactory()

    def test_filter_data_permissions(self):
        request = self.factory.get('/')
        request.user = self.user
        request.query_params = {}

        serializer = EntitySerializer(self.entity, context={'request': request})
        self.assertEqual(serializer.data['data'], [self.data_1.pk])


class DictRelatedFieldTest(TestCase):
    def setUp(self):
        super().setUp()

        self.process = Process.objects.create(
            slug='test-process',
            contributor=self.contributor,
        )
        assign_perm('view_process', self.user, self.process)

        self.descriptor_schema1 = DescriptorSchema.objects.create(
            slug='test-schema',
            contributor=self.contributor,
            version='1.0.0',
        )
        assign_perm('view_descriptorschema', self.user, self.descriptor_schema1)

        self.descriptor_schema2 = DescriptorSchema.objects.create(
            slug='test-schema',
            contributor=self.contributor,
            version='2.0.0',
        )
        assign_perm('view_descriptorschema', self.user, self.descriptor_schema2)

        self.descriptor_schema3 = DescriptorSchema.objects.create(
            slug='test-schema',
            contributor=self.contributor,
            version='3.0.0',
        )

        self.factory = APIRequestFactory()

    def test_to_internal_value(self):
        request = self.factory.get('/')
        request.user = self.user
        request.query_params = {}

        # serializer = DataSerializer(context={'request': request}, data={
        #     'contributor': self.user.pk,
        #     'process': {'id': self.process.id},
        #     'descriptor_schema': {'slug': 'test-schema'},
        # })
        # # is_valid() needs to be called before accessing ``validated_data``
        # serializer.is_valid()
        # self.assertEqual(serializer.validated_data['process'], self.process)
        # # Check that descriptor schema with highest version & view permission is used:
        # self.assertEqual(serializer.validated_data['descriptor_schema'], self.descriptor_schema2)

        serializer = DataSerializer(context={'request': request}, data={
            'contributor': self.user.pk,
            'process': {'id': self.process.id},
            'descriptor_schema': {},
        })
        # is_valid() needs to be called before accessing ``validated_data``
        serializer.is_valid()
        msg = 'Neither id nor slug is given for field descriptor_schema.'
        self.assertEqual(serializer.errors['descriptor_schema'], [msg])

        serializer = DataSerializer(context={'request': request}, data={
            'contributor': self.user.pk,
            'process': {'id': self.process.id},
            'descriptor_schema': {'slug': 'unexisting-slug'},
        })
        # is_valid() needs to be called before accessing ``validated_data``
        serializer.is_valid()
        msg = "Invalid descriptorschema value: {'slug': 'unexisting-slug'} - object does not exist."
        self.assertEqual(serializer.errors['descriptor_schema'], [msg])

    def test_to_representation(self):
        request = self.factory.get('/')
        request.user = self.user
        request.query_params = {}

        data = Data.objects.create(
            contributor=self.user,
            process=self.process,
            descriptor_schema=self.descriptor_schema1,
        )

        serializer = DataSerializer(data, context={'request': request})
        self.assertEqual(serializer.data['process'], {
            'id': self.process.pk,
            'slug': self.process.slug,
        })

        # Check that descriptor_schema is properly hydrated:
        self.assertDictEqual(serializer.data['descriptor_schema'], {
            'id': self.descriptor_schema1.id,
            'slug': 'test-schema',
            'version': '1.0.0',
            'name': '',
            'description': '',
            'created': self.descriptor_schema1.created.isoformat(),
            'modified': self.descriptor_schema1.modified.isoformat(),
            'schema': [],
            'contributor': OrderedDict([
                ('first_name', 'Joe'),
                ('id', serializer.data['descriptor_schema']['contributor']['id']),
                ('last_name', 'Miller'),
                ('username', 'contributor'),
            ]),
        })
