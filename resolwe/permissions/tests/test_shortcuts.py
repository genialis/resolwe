# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import unittest
import six

from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser, Group

from guardian.shortcuts import assign_perm
from guardian.models import GroupObjectPermission, UserObjectPermission

from resolwe.flow.models import Collection
from resolwe.permissions.shortcuts import get_user_group_perms, get_object_perms


class UserGroupTestCase(unittest.TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create(username="test_user")
        self.group1 = Group.objects.create(name="Test group 1")
        self.group2 = Group.objects.create(name="Test group 2")

        self.collection = Collection.objects.create(
            contributor=self.user,
            name="Test collection",
        )

        # This collection is here to make sure that other permissions
        # don't affect tested queries.
        collection2 = Collection.objects.create(
            contributor=self.user,
            name="Test collection 2",
        )
        assign_perm("view_collection", self.user, collection2)
        assign_perm("view_collection", self.group1, collection2)

    def tearDown(self):
        GroupObjectPermission.objects.all().delete()
        UserObjectPermission.objects.all().delete()
        Collection.objects.all().delete()
        Group.objects.all().delete()
        # `public` user is created by guardian
        get_user_model().objects.exclude(username="public").delete()

    def test_user(self):
        assign_perm("view_collection", self.user, self.collection)
        assign_perm("edit_collection", self.user, self.collection)

        user_perms, group_perms = get_user_group_perms(self.user, self.collection)

        self.assertEqual(len(group_perms), 0)
        six.assertCountEqual(self, user_perms, ["view_collection", "edit_collection"])

    def test_user_in_group(self):
        self.group1.user_set.add(self.user)
        assign_perm("view_collection", self.group1, self.collection)
        assign_perm("edit_collection", self.group1, self.collection)

        user_perms, group_perms = get_user_group_perms(self.user, self.collection)
        self.assertEqual(len(group_perms), 1)
        six.assertCountEqual(self, group_perms[0][2], ["view_collection", "edit_collection"])
        self.assertEqual(len(user_perms), 0)

        assign_perm("view_collection", self.user, self.collection)

        user_perms, group_perms = get_user_group_perms(self.user, self.collection)
        self.assertEqual(len(group_perms), 1)
        six.assertCountEqual(self, group_perms[0][2], ["view_collection", "edit_collection"])
        self.assertEqual(len(user_perms), 1)
        six.assertCountEqual(self, user_perms, ["view_collection"])

    def test_user_in_multiple_groups(self):
        self.group1.user_set.add(self.user)
        self.group2.user_set.add(self.user)
        assign_perm("view_collection", self.group1, self.collection)
        assign_perm("edit_collection", self.group1, self.collection)
        assign_perm("view_collection", self.group2, self.collection)

        user_perms, group_perms = get_user_group_perms(self.user, self.collection)
        self.assertEqual(len(group_perms), 2)
        self.assertEqual(group_perms[0][0], self.group1.pk)
        six.assertCountEqual(self, group_perms[0][2], ["view_collection", "edit_collection"])
        self.assertEqual(group_perms[1][0], self.group2.pk)
        six.assertCountEqual(self, group_perms[1][2], ["view_collection"])
        self.assertEqual(len(user_perms), 0)

    def test_group(self):
        assign_perm("view_collection", self.group1, self.collection)
        assign_perm("edit_collection", self.group1, self.collection)
        user_perms, group_perms = get_user_group_perms(self.group1, self.collection)
        self.assertEqual(len(group_perms), 1)
        six.assertCountEqual(self, group_perms[0][2], ["view_collection", "edit_collection"])
        self.assertEqual(len(user_perms), 0)


class ObjectPermsTestCase(unittest.TestCase):
    def setUp(self):
        self.user1 = get_user_model().objects.create(username="test_user1")
        self.user2 = get_user_model().objects.create(username="test_user2")
        self.group1 = Group.objects.create(name="Test group 1")
        self.group2 = Group.objects.create(name="Test group 2")
        self.anonymous = AnonymousUser()

        self.collection = Collection.objects.create(
            contributor=self.user1,
            name="Test collection",
        )

    def tearDown(self):
        GroupObjectPermission.objects.all().delete()
        UserObjectPermission.objects.all().delete()
        Collection.objects.all().delete()
        Group.objects.all().delete()
        # `public` user is created by guardian
        get_user_model().objects.exclude(username="public").delete()

    def test_all_permissions(self):
        self.group1.user_set.add(self.user1)

        perms = get_object_perms(self.collection)
        self.assertEqual(len(perms), 0)

        assign_perm("view_collection", self.user1, self.collection)
        assign_perm("edit_collection", self.user1, self.collection)
        assign_perm("view_collection", self.user2, self.collection)
        expected_perms = [
            {'permissions': ['edit', 'view'], 'type': 'user', 'id': self.user1.pk, 'name': 'test_user1'},
            {'permissions': ['view'], 'type': 'user', 'id': self.user2.pk, 'name': 'test_user2'},
        ]
        perms = get_object_perms(self.collection)
        six.assertCountEqual(self, expected_perms, perms)

        assign_perm("view_collection", self.group1, self.collection)
        assign_perm("edit_collection", self.group1, self.collection)
        assign_perm("view_collection", self.group2, self.collection)
        expected_perms.extend([
            {'permissions': ['edit', 'view'], 'type': 'group', 'id': self.group1.pk, 'name': 'Test group 1'},
            {'permissions': ['view'], 'type': 'group', 'id': self.group2.pk, 'name': 'Test group 2'},
        ])
        perms = get_object_perms(self.collection)
        six.assertCountEqual(self, expected_perms, perms)

        assign_perm("view_collection", self.anonymous, self.collection)
        expected_perms.append(
            {'permissions': ['view'], 'type': 'public'},
        )
        perms = get_object_perms(self.collection)
        six.assertCountEqual(self, expected_perms, perms)

    def test_user_permissions(self):
        self.group1.user_set.add(self.user1)
        assign_perm("view_collection", self.user1, self.collection)
        assign_perm("edit_collection", self.user1, self.collection)
        assign_perm("view_collection", self.user2, self.collection)
        assign_perm("view_collection", self.group1, self.collection)
        assign_perm("edit_collection", self.group1, self.collection)
        assign_perm("view_collection", self.group2, self.collection)

        expected_perms = [
            {'permissions': ['edit', 'view'], 'type': 'user', 'id': self.user1.pk, 'name': 'test_user1'},
            {'permissions': ['edit', 'view'], 'type': 'group', 'id': self.group1.pk, 'name': 'Test group 1'},
        ]
        perms = get_object_perms(self.collection, self.user1)
        six.assertCountEqual(self, expected_perms, perms)

        self.group2.user_set.add(self.user1)
        expected_perms.append(
            {'permissions': ['view'], 'type': 'group', 'id': self.group2.pk, 'name': 'Test group 2'},
        )
        perms = get_object_perms(self.collection, self.user1)
        six.assertCountEqual(self, expected_perms, perms)

        assign_perm("view_collection", self.anonymous, self.collection)
        expected_perms.append(
            {'permissions': ['view'], 'type': 'public'},
        )
        perms = get_object_perms(self.collection, self.user1)
        six.assertCountEqual(self, expected_perms, perms)
