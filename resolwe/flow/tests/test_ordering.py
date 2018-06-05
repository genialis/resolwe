# pylint: disable=missing-docstring
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser
from django.core.urlresolvers import reverse

from guardian.shortcuts import assign_perm
from rest_framework.test import APITestCase

from resolwe.flow.models import Process


class ProcessOrderingTest(APITestCase):

    def setUp(self):
        super().setUp()

        user_model = get_user_model()
        user = user_model.objects.create(username='user')

        self.proc_1 = Process.objects.create(name='My process', contributor=user, version=1)
        self.proc_2 = Process.objects.create(name='My process', contributor=user, version=2)

        assign_perm('view_process', AnonymousUser(), self.proc_1)
        assign_perm('view_process', AnonymousUser(), self.proc_2)

        self.url = reverse('resolwe-api:process-list')

    def test_ordering_version(self):
        # pylint: disable=no-member
        response = self.client.get(self.url, {'ordering': 'version'}, format='json')
        self.assertEqual(response.data[0]['id'], self.proc_1.id)
        self.assertEqual(response.data[1]['id'], self.proc_2.id)

        response = self.client.get(self.url, {'ordering': '-version'}, format='json')
        self.assertEqual(response.data[0]['id'], self.proc_2.id)
        self.assertEqual(response.data[1]['id'], self.proc_1.id)
