# pylint: disable=missing-docstring
from guardian.models import GroupObjectPermission, UserObjectPermission
from guardian.shortcuts import assign_perm

from resolwe.flow.models import Collection, DescriptorSchema, Process
from resolwe.permissions.utils import change_perm_ctype, copy_permissions, get_full_perm, get_perm_action
from resolwe.test import TestCase


class UtilsTestCase(TestCase):
    def setUp(self):
        super().setUp()

        self.src_process = Process.objects.create(name='Source process', contributor=self.contributor)
        self.dst_process = Process.objects.create(name='Destination process', contributor=self.contributor)

        self.collection = Collection.objects.create(name='Test collection', contributor=self.contributor)

        self.descriptor_schema = DescriptorSchema.objects.create(contributor=self.contributor)

    def test_get_perm_action(self):
        self.assertEqual(get_perm_action('view_data'), 'view')
        self.assertEqual(get_perm_action('view'), 'view')

    def test_get_full_perm(self):
        self.assertEqual(get_full_perm('view', self.collection), 'view_collection')
        self.assertEqual(get_full_perm('view', self.descriptor_schema), 'view_descriptorschema')

    def test_change_perm_ctype(self):
        self.assertEqual(change_perm_ctype('view_data', self.collection), 'view_collection')

    def test_copy_permissions(self):
        assign_perm('view_process', self.contributor, self.src_process)
        assign_perm('view_process', self.group, self.src_process)

        copy_permissions(self.src_process, self.dst_process)

        self.assertEqual(GroupObjectPermission.objects.count(), 2)
        self.assertEqual(UserObjectPermission.objects.count(), 2)

        self.assertTrue(self.contributor.has_perm('flow.view_process', self.dst_process))
        # User inherites permission from group
        self.assertTrue(self.user.has_perm('flow.view_process', self.dst_process))

    def test_copy_different_ctype(self):
        assign_perm('view_collection', self.contributor, self.collection)
        assign_perm('add_collection', self.contributor, self.collection)

        copy_permissions(self.collection, self.dst_process)

        # Only 'view' is copied as process has no 'add' permission.
        self.assertEqual(UserObjectPermission.objects.count(), 3)
