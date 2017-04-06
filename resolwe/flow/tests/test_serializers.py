# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from guardian.shortcuts import assign_perm
from rest_framework.test import APIRequestFactory

from resolwe.flow.models import Collection, Data, Entity, Process
from resolwe.flow.serializers import CollectionSerializer, EntitySerializer
from resolwe.test import TestCase


class CollectionSerializerTest(TestCase):
    def setUp(self):
        super(CollectionSerializerTest, self).setUp()

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
        super(EntitySerializerTest, self).setUp()

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
