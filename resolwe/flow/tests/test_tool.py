# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from django.contrib.auth import get_user_model
from django.db import IntegrityError, transaction
from django.test import TestCase

from resolwe.flow.models import Process


class ManagerTest(TestCase):

    def setUp(self):
        self.u = get_user_model().objects.create_superuser('test', 'test@genialis.com', 'test')
        self.data = {'slug': 'test-process',
                     'name': 'Test Process',
                     'contributor': self.u,
                     'type': 'data:test',
                     'version': 1}

    def test_unique(self):
        Process(**self.data).save()

        self.data['version'] = 2
        Process(**self.data).save()

        with transaction.atomic():
            self.assertRaises(IntegrityError, Process(**self.data).save)

        self.data['slug'] = 'test-processor-1'
        Process(**self.data).save()
