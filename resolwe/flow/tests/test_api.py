# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import mock
import unittest

from django.contrib.auth.models import User

from rest_framework.test import APIRequestFactory, force_authenticate

from resolwe.flow.models import Data, Process, Collection
from resolwe.flow.views import DataViewSet, CollectionViewSet

factory = APIRequestFactory()


class TestDataViewSetCase(unittest.TestCase):
    def setUp(self):
        self.data_viewset = DataViewSet.as_view(actions={
            'get': 'list',
        })

        self.user = User.objects.create(is_superuser=True)

    def tearDown(self):
        Data.objects.all().delete()
        Process.objects.all().delete()
        self.user.delete()

    @mock.patch('resolwe.flow.models.Process.objects.all')
    def test_prefetch(self, process_mock):
        # TODO: find way to mock Data objects
        proc = Process.objects.create(type='test:process', contributor=self.user)
        Data.objects.create(contributor=self.user, slug='test1', process=proc)
        Data.objects.create(contributor=self.user, slug='test2', process=proc)

        request = factory.get('/', '', content_type='application/json')
        force_authenticate(request, self.user)
        self.data_viewset(request)

        # check that only one request is made to get all processes' types
        self.assertEqual(process_mock.call_count, 1)


class TestCollectionViewSetCase(unittest.TestCase):
    def setUp(self):
        self.checkslug_viewset = CollectionViewSet.as_view(actions={
            'get': 'slug_exists',
        })

        self.user = User.objects.create(is_superuser=True)

    def tearDown(self):
        Collection.objects.all().delete()
        self.user.delete()

    def test_check_slug(self):
        Collection.objects.create(slug="collection1", name="Collection 1", contributor=self.user)

        # unauthorized
        request = factory.get('/', {'name': 'collection1'}, content_type='application/json')
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.status_code, 401)
        self.assertEqual(resp.data, None)

        # existing slug
        request = factory.get('/', {'name': 'collection1'}, content_type='application/json')
        force_authenticate(request, self.user)
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.data, True)

        # existing slug - iexact
        request = factory.get('/', {'name': 'Collection1'}, content_type='application/json')
        force_authenticate(request, self.user)
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.data, True)

        # non-existing slug
        request = factory.get('/', {'name': 'new-collection'}, content_type='application/json')
        force_authenticate(request, self.user)
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.data, False)

        # bad query parameter
        request = factory.get('/', {'bad': 'parameter'}, content_type='application/json')
        force_authenticate(request, self.user)
        resp = self.checkslug_viewset(request)
        self.assertEqual(resp.status_code, 400)
