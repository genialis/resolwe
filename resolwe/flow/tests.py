from django.core.urlresolvers import reverse
from django.test import TestCase

from rest_framework.test import APIRequestFactory


class ProjectResourceTest(TestCase):
    def setUp(self):
        self.api_client = APIRequestFactory()
        self.list_url = reverse('resolwe-api:project-list')
        # TODO: add object's id
        # self.detail_url = reverse('resolwe-api:project-detail')

    def test_readonly_fields(self):
        self.api_client.get(self.list_url)
