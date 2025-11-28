# pylint: disable=missing-docstring
from django.utils.text import slugify

from resolwe.flow.models import Process
from resolwe.flow.models.base import MAX_SLUG_LENGTH
from resolwe.flow.models.fields import MAX_SLUG_SEQUENCE_DIGITS
from resolwe.test import TestCase


class ManagerTest(TestCase):
    def setUp(self):
        super().setUp()

        self.data = {
            "name": "Test Process",
            "contributor": self.contributor,
            "type": "data:test",
            "version": 1,
        }

    def test_slug(self):
        p = Process.objects.create(**self.data)
        self.assertEqual(p.slug, "test-process")

        self.data["version"] = 2
        p = Process.objects.create(**self.data)
        self.assertEqual(p.slug, "test-process")

        p = Process.objects.create(**self.data)
        self.assertEqual(p.slug, "test-process-2")

        # Make sure slug is truncated properly.
        data = self.data.copy()
        data["name"] = "N" * (MAX_SLUG_LENGTH - MAX_SLUG_SEQUENCE_DIGITS - 2) + "-name"
        p = Process.objects.create(**data)
        self.assertEqual(p.slug, slugify(p.slug))
