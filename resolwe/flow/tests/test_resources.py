# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from django.core.urlresolvers import reverse
from django.test import TestCase

from rest_framework.test import APIRequestFactory


class CollectionResourceTest(TestCase):
    def setUp(self):
        self.api_client = APIRequestFactory()
        self.list_url = reverse('resolwe-api:collection-list')
        # TODO: add object's id
        # self.detail_url = reverse('resolwe-api:collection-detail')

    def test_readonly_fields(self):
        self.api_client.get(self.list_url)
