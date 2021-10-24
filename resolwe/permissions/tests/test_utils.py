# pylint: disable=missing-docstring

from resolwe.flow.models import Collection, DescriptorSchema, Process
from resolwe.permissions.models import Permission
from resolwe.permissions.utils import copy_permissions
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
        self.src_process.set_permission(Permission.VIEW, self.contributor)
        self.src_process.set_permission(Permission.VIEW, self.group)

        copy_permissions(self.src_process, self.dst_process)
        self.assertTrue(self.contributor.has_perm(Permission.VIEW, self.dst_process))
        # User inherites permission from group
        self.assertTrue(self.user.has_perm(Permission.VIEW, self.dst_process))
