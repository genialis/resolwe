# pylint: disable=missing-docstring
from resolwe.flow.models import Process
from resolwe.test import TestCase


class ManagerTest(TestCase):

    def setUp(self):
        super().setUp()

        self.data = {'name': 'Test Process',
                     'contributor': self.contributor,
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
