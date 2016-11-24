# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import mock

from django.contrib.auth.models import User
from django.core.urlresolvers import reverse
from django.test import TestCase

from guardian.shortcuts import assign_perm, remove_perm
from rest_framework.test import APIRequestFactory, force_authenticate
from rest_framework import status

from resolwe.flow.models import Data, Process, Collection, DescriptorSchema
from resolwe.flow.views import DataViewSet, CollectionViewSet

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

        self.user = User.objects.create(is_superuser=True)

        proc = Process.objects.create(type='test:process', name='Test process', contributor=self.user)
        self.data_1 = Data.objects.create(contributor=self.user, slug='test1', process=proc)
        self.data_2 = Data.objects.create(contributor=self.user, slug='test2', process=proc)

    @mock.patch('resolwe.flow.models.Process.objects.all')
    def test_prefetch(self, process_mock):
        request = factory.get('/', '', format='json')
        force_authenticate(request, self.user)
        self.data_viewset(request)

        # check that only one request is made to get all processes' types
        self.assertEqual(process_mock.call_count, 1)


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

        self.super_user = User.objects.create(username='superuser', is_superuser=True)
        self.user = User.objects.create(username='normaluser')

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
