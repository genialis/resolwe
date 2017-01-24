# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import mock

from django.contrib.auth import get_user_model
from django.core.urlresolvers import reverse
from django.db import DEFAULT_DB_ALIAS, connections
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from guardian.shortcuts import assign_perm, remove_perm
from rest_framework.test import APIRequestFactory, force_authenticate
from rest_framework import exceptions, status

from resolwe.flow.models import Data, Entity, Process, Collection, DescriptorSchema
from resolwe.flow.views import DataViewSet, CollectionViewSet, EntityViewSet

factory = APIRequestFactory()  # pylint: disable=invalid-name


MESSAGES = {
    'NOT_FOUND': 'Not found.',
    'NO_PERMS': 'You do not have permission to perform this action.',
}


class TestDataViewSetCase(TestCase):
    def setUp(self):
        self.data_viewset = DataViewSet.as_view(actions={
            'get': 'list',
        })

        user_model = get_user_model()
        self.user = user_model.objects.create(is_superuser=True)
        self.proc = Process.objects.create(type='test:process', name='Test process', contributor=self.user)

    def test_prefetch(self):
        request = factory.get('/', '', format='json')
        force_authenticate(request, self.user)

        for _ in range(10):
            Data.objects.create(contributor=self.user, process=self.proc)

        # Check prefetch. The number of queries without prefetch depends
        # on the number of Data objects. With prefetch 56 queries,
        # without prefetch 73 queries. Python 2 and 3 have slightly
        # different number of queries, so we set a loose constraint in test.
        conn = connections[DEFAULT_DB_ALIAS]
        with CaptureQueriesContext(conn) as captured_queries:
            self.data_viewset(request)
            self.assertLess(len(captured_queries), 62)


class TestCollectionViewSetCase(TestCase):
    def setUp(self):
        self.checkslug_viewset = CollectionViewSet.as_view(actions={
            'get': 'slug_exists',
        })
        self.add_data_viewset = CollectionViewSet.as_view(actions={
            'post': 'add_data',
        })
        self.remove_data_viewset = CollectionViewSet.as_view(actions={
            'post': 'remove_data',
        })
        self.collection_detail_viewset = CollectionViewSet.as_view(actions={
            'get': 'retrieve',
            'put': 'update',
            'patch': 'partial_update',
            'delete': 'destroy',
        })
        self.collection_list_viewset = CollectionViewSet.as_view(actions={
            'get': 'list',
            'post': 'create',
        })

        self.detail_url = lambda pk: reverse('resolwe-api:collection-detail', kwargs={'pk': pk})

        user_model = get_user_model()
        self.super_user = user_model.objects.create(username='superuser', is_superuser=True)
        self.user = user_model.objects.create(username='normaluser')

    def test_set_descriptor_schema(self):
        d_schema = DescriptorSchema.objects.create(slug="new-schema", name="New Schema", contributor=self.user)

        data = {
            'name': 'Test collection',
            'descriptor_schema': 'new-schema',
        }

        request = factory.post('/', data=data, format='json')
        force_authenticate(request, self.super_user)
        self.collection_list_viewset(request)

        self.assertEqual(Collection.objects.count(), 1)
        self.assertEqual(Collection.objects.first().descriptor_schema, d_schema)

    def test_change_descriptor_schema(self):
        collection = Collection.objects.create(slug="collection1", name="Collection 1", contributor=self.user)
        d_schema = DescriptorSchema.objects.create(slug="new-schema", name="New Schema", contributor=self.user)

        request = factory.patch(self.detail_url(collection.pk), {'descriptor_schema': 'new-schema'}, format='json')
        force_authenticate(request, self.super_user)
        self.collection_detail_viewset(request, pk=collection.pk)

        collection.refresh_from_db()
        self.assertEqual(collection.descriptor_schema, d_schema)

    def test_check_slug(self):
        Collection.objects.create(slug="collection1", name="Collection 1", contributor=self.super_user)

        # unauthorized
        request = factory.get('/', {'name': 'collection1'}, format='json')
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.status_code, 401)
        self.assertEqual(resp.data, None)

        # existing slug
        request = factory.get('/', {'name': 'collection1'}, format='json')
        force_authenticate(request, self.super_user)
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.data, True)

        # existing slug - iexact
        request = factory.get('/', {'name': 'Collection1'}, format='json')
        force_authenticate(request, self.super_user)
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.data, True)

        # non-existing slug
        request = factory.get('/', {'name': 'new-collection'}, format='json')
        force_authenticate(request, self.super_user)
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.data, False)

        # bad query parameter
        request = factory.get('/', {'bad': 'parameter'}, format='json')
        force_authenticate(request, self.super_user)
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.status_code, 400)

    def test_add_remove_data(self):
        c = Collection.objects.create(slug="collection1", name="Collection 1", contributor=self.super_user)
        assign_perm('view_collection', self.user, c)
        assign_perm('edit_collection', self.user, c)
        assign_perm('share_collection', self.user, c)

        proc = Process.objects.create(type='test:process', name='Test process', contributor=self.user)
        d = Data.objects.create(contributor=self.user, slug='test1', process=proc)

        request = factory.post(self.detail_url(c.pk), {'ids': [str(d.pk)]}, format='json')
        force_authenticate(request, self.user)

        # user w/o permissions cannot add data
        resp = self.add_data_viewset(request, pk=c.pk)
        self.assertEqual(resp.data[u'detail'], MESSAGES['NO_PERMS'])
        self.assertEqual(c.data.count(), 0)

        assign_perm('add_collection', self.user, c)

        # user w/ permissions can add data
        resp = self.add_data_viewset(request, pk=c.pk)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(c.data.count(), 1)

        request = factory.post(self.detail_url(c.pk), {'ids': [str(d.pk)]}, format='json')
        force_authenticate(request, self.user)
        remove_perm('add_collection', self.user, c)

        # user w/o permissions cannot remove data
        resp = self.remove_data_viewset(request, pk=c.pk)
        self.assertEqual(resp.data[u'detail'], MESSAGES['NO_PERMS'])
        self.assertEqual(c.data.count(), 1)

        assign_perm('add_collection', self.user, c)

        # user w/ permissions can remove data
        resp = self.remove_data_viewset(request, pk=c.pk)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(c.data.count(), 0)

        request = factory.post(self.detail_url(c.pk), {'ids': ['42']}, format='json')
        force_authenticate(request, self.user)

        resp = self.remove_data_viewset(request, pk=c.pk)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(c.data.count(), 0)


class EntityViewSetTest(TestCase):
    def setUp(self):
        user_model = get_user_model()

        self.user = user_model.objects.create_user('test_user')
        self.collection = Collection.objects.create(name="Test Collection", contributor=self.user)
        self.entity = Entity.objects.create(name="Test entity", contributor=self.user)
        process = Process.objects.create(name="Test process", contributor=self.user)
        self.data = Data.objects.create(name="Test data", contributor=self.user, process=process)
        self.data_2 = Data.objects.create(name="Test data 2", contributor=self.user, process=process)

        # another Data object to make sure that other objects are not processed
        Data.objects.create(name="Dummy data", contributor=self.user, process=process)

        self.entity.data.add(self.data)

        assign_perm('add_collection', self.user, self.collection)
        assign_perm('add_entity', self.user, self.entity)

        self.entityviewset = EntityViewSet()

    def test_add_to_collection(self):
        request_mock = mock.MagicMock(data={'ids': [self.collection.pk]}, user=self.user)
        self.entityviewset.get_object = lambda: self.entity

        self.entityviewset.add_to_collection(request_mock)

        self.assertEqual(self.collection.data.count(), 1)
        self.assertEqual(self.entity.collections.count(), 1)

    def test_remove_from_collection(self):
        # Manually add Entity and it's Data objects to the Collection
        self.entity.collections.add(self.collection.pk)
        self.collection.data.add(self.data)

        request_mock = mock.MagicMock(data={'ids': [self.collection.pk]}, user=self.user)
        self.entityviewset.get_object = lambda: self.entity

        self.entityviewset.remove_from_collection(request_mock)

        self.assertEqual(self.collection.data.count(), 0)
        self.assertEqual(self.entity.collections.count(), 0)

    def test_add_remove_permissions(self):
        request_mock = mock.MagicMock(data={'ids': [self.collection.pk]}, user=self.user)
        self.entityviewset.get_object = lambda: self.entity

        remove_perm('add_collection', self.user, self.collection)

        with self.assertRaises(exceptions.PermissionDenied):
            self.entityviewset.remove_from_collection(request_mock)

        with self.assertRaises(exceptions.PermissionDenied):
            self.entityviewset.add_to_collection(request_mock)

    def test_add_data(self):
        self.entity.collections.add(self.collection)

        request_mock = mock.MagicMock(data={'ids': [self.data.pk]}, user=self.user)
        self.entityviewset.get_object = lambda: self.entity

        self.entityviewset.add_data(request_mock)

        self.assertEqual(self.entity.data.count(), 1)
        self.assertEqual(self.collection.data.count(), 1)

    def test_remove_data(self):
        self.entity.data.add(self.data_2)
        self.entityviewset.get_object = lambda: self.entity

        # entity is removed only when last data object is removed
        request_mock = mock.MagicMock(data={'ids': [self.data.pk]}, user=self.user)
        self.entityviewset.remove_data(request_mock)
        self.assertEqual(Entity.objects.count(), 1)
        request_mock = mock.MagicMock(data={'ids': [self.data_2.pk]}, user=self.user)
        self.entityviewset.remove_data(request_mock)
        self.assertEqual(Entity.objects.count(), 0)
