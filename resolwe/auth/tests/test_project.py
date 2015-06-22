# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from django.core.urlresolvers import reverse

from rest_framework.test import APITestCase, force_authenticate


class ProjectTestCase(APITestCase):
    fixtures = ['users.json', 'projects.json']

    def setUp(self):
        super(ProjectTestCase, self).setUp()

        self.list_url = reverse('resolwe-api:project-list')

    def test_unauthenticated(self):
        resp = self.client.get(self.list_url)

        print(resp.data)

        self.assertEqual(len(resp.data), 0)
