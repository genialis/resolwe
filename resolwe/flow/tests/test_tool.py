# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from django.contrib.auth import get_user_model
from django.db import IntegrityError, transaction
from django.test import TestCase

from resolwe.flow.models import Process


class ManagerTest(TestCase):

    def setUp(self):
        self.u = get_user_model().objects.create_superuser('test', 'test@genialis.com', 'test')
        self.data = {'name': 'Test Process',
                     'contributor': self.u,
                     'type': 'data:test',
                     'version': 1}

    def test_slug(self):
        p = Process.objects.create(**self.data)
        self.assertEqual(p.slug, 'test-process')

        self.data['version'] = 2
        p = Process.objects.create(**self.data)
        self.assertEqual(p.slug, 'test-process')

        p = Process.objects.create(**self.data)
        self.assertEqual(p.slug, 'test-process-2')
