# pylint: disable=missing-docstring

from resolwe.flow.models import Collection, DescriptorSchema, Process
from resolwe.permissions.utils import copy_permissions, get_perms, set_permission
from resolwe.test import TestCase


class UtilsTestCase(TestCase):
    def setUp(self):
        super().setUp()

        self.src_process = Process.objects.create(
            name="Source process", contributor=self.contributor
        )
        self.dst_process = Process.objects.create(
            name="Destination process", contributor=self.contributor
        )

        self.collection = Collection.objects.create(
            name="Test collection", contributor=self.contributor
        )

        self.descriptor_schema = DescriptorSchema.objects.create(
            contributor=self.contributor
        )

    def test_copy_permissions(self):
        self.src_process.set_permission("view", self.contributor)
        self.src_process.set_permission("view", self.group)

        copy_permissions(self.src_process, self.dst_process)
        self.assertTrue(self.contributor.has_perm("flow.view", self.dst_process))
        # User inherites permission from group
        self.assertTrue(self.user.has_perm("flow.view", self.dst_process))

    def test_copy_different_ctype(self):
        # TODO: this could be annoying. Even assigning 'edit' to process might work.

        set_permission("edit", self.contributor, self.collection)

        copy_permissions(self.collection, self.dst_process)

        self.assertEqual(get_perms(self.contributor, self.dst_process), ["view"])
