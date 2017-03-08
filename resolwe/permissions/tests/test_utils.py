# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from guardian.models import GroupObjectPermission, UserObjectPermission
from guardian.shortcuts import assign_perm

from resolwe.flow.models import Collection, Process
from resolwe.permissions.utils import copy_permissions
from resolwe.test import TestCase


class UtilsTestCase(TestCase):
    def setUp(self):
        super(UtilsTestCase, self).setUp()

        self.src_process = Process.objects.create(name='Source process', contributor=self.contributor)
        self.dst_process = Process.objects.create(name='Destination process', contributor=self.contributor)

        self.collection = Collection.objects.create(name='Test collection', contributor=self.contributor)

    def test_copy_permissions(self):
        assign_perm('view_process', self.contributor, self.src_process)
        assign_perm('view_process', self.group, self.src_process)

        copy_permissions(self.src_process, self.dst_process)

        self.assertEqual(GroupObjectPermission.objects.count(), 2)
        self.assertEqual(UserObjectPermission.objects.count(), 2)

        self.assertTrue(self.contributor.has_perm('flow.view_process', self.dst_process))
        # User inherites permission from group
        self.assertTrue(self.user.has_perm('flow.view_process', self.dst_process))

    def test_copy_perms_wrong_ctype(self):
        with self.assertRaises(AssertionError):
            copy_permissions(self.src_process, self.collection)
