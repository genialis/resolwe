# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import mock
import unittest

from django.contrib.auth.models import User

from rest_framework.test import APIRequestFactory, force_authenticate

from resolwe.flow.models import Data, Process
from resolwe.flow.views import DataViewSet

factory = APIRequestFactory()


class TestDataViewSetCase(unittest.TestCase):
    def setUp(self):
        self.data_viewset = DataViewSet.as_view(actions={
            'get': 'list',
        })

        self.user = User.objects.create(is_superuser=True)

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
