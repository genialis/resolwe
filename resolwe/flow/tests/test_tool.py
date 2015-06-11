# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from django.contrib.auth import get_user_model
from django.db import IntegrityError, transaction
from django.test import TestCase

from resolwe.flow.models import Tool


class ManagerTest(TestCase):

    def setUp(self):
        self.u = get_user_model().objects.create_superuser('test', 'test@genialis.com', 'test')

    def test_unique(self):
        t = {'slug': 'test-processor',
             'name': 'Test Processor',
             'contributor': self.u,
             'type': 'data:test',
             'version': 1}

        Tool(**t).save()

        t['version'] = 2
        Tool(**t).save()

        with transaction.atomic():
            self.assertRaises(IntegrityError, Tool(**t).save)

        t['slug'] = 'test-processor-1'
        Tool(**t).save()
