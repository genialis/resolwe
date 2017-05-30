# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from copy import deepcopy

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group

from guardian.models import GroupObjectPermission, UserObjectPermission
from guardian.shortcuts import assign_perm
from rest_framework import exceptions, status

from resolwe.flow.models import Collection, Data, Entity, Process
from resolwe.flow.views import CollectionViewSet
from resolwe.permissions.utils import check_owner_permission, check_public_permissions, check_user_permissions
from resolwe.test import ResolweAPITestCase, TestCase


class CollectionPermissionsTest(ResolweAPITestCase):

    def setUp(self):
        self.user1 = get_user_model().objects.create(username="test_user1")
        self.user2 = get_user_model().objects.create(username="test_user2")
        # public user is already created bt django-guardian
        self.public = get_user_model().objects.get(username="public")

        self.group = Group.objects.create(name="Test group")

        self.collection = Collection.objects.create(
            contributor=self.user1,
            name="Test collection 1"
        )

        self.resource_name = 'collection'
        self.viewset = CollectionViewSet

        super(CollectionPermissionsTest, self).setUp()

    def _create_data(self):
        process = Process.objects.create(
            name='Test process',
            contributor=self.user1,
        )

        return Data.objects.create(
            name='Test data',
            contributor=self.user1,
            process=process,
        )

    def _create_entity(self):
        return Entity.objects.create(
            name='Test entity',
            contributor=self.user1,
        )

    def test_public_user(self):
        """Public user cannot create/edit anything"""
        assign_perm("view_collection", self.user1, self.collection)
        assign_perm("share_collection", self.user1, self.collection)

        data = {'public': {'add': ['view']}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)

        data = {'public': {'add': ['edit']}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)

        data = {'public': {'remove': ['edit']}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)

    def test_user_without_share(self):
        """User without ``SHARE`` permission cannot do anything"""
        assign_perm("view_collection", self.user1, self.collection)
        assign_perm("download_collection", self.user1, self.collection)
        assign_perm("add_collection", self.user1, self.collection)
        assign_perm("edit_collection", self.user1, self.collection)

        # Can not add permissions to users.
        data = {'users': {'add': {self.user2.pk: ['view']}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)

        # Can not add permissions to groups.
        data = {'users': {'add': {self.group.pk: ['view']}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)

        # Can not remove permissions from users.
        data = {'users': {'remove': {self.user2.pk: ['view']}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)

        # Can not remove permissions from groups.
        data = {'users': {'remove': {self.group.pk: ['view']}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)

    def test_user_with_share(self):
        assign_perm("view_collection", self.user1, self.collection)
        assign_perm("share_collection", self.user1, self.collection)

        # Can add permissions to users.
        data = {'users': {'add': {self.user2.pk: ['view']}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.filter(user=self.user2).count(), 1)

        # Can add permissions to groups.
        data = {'groups': {'add': {self.group.pk: ['view']}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(GroupObjectPermission.objects.count(), 1)

        # Can remove permissions from users.
        data = {'users': {'remove': {self.user2.pk: ['view']}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.filter(user=self.user2).count(), 0)

        # Can remove permissions from groups.
        data = {'groups': {'remove': {self.group.pk: ['view']}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(GroupObjectPermission.objects.count(), 0)

    def test_protect_owner(self):
        """Only owners can modify ``owner``permission"""
        assign_perm("view_collection", self.user1, self.collection)
        assign_perm("download_collection", self.user1, self.collection)
        assign_perm("add_collection", self.user1, self.collection)
        assign_perm("edit_collection", self.user1, self.collection)
        assign_perm("share_collection", self.user1, self.collection)

        # User with share permission cannot grant ``owner`` permission
        data = {'users': {'add': {self.user2.pk: ['owner']}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(UserObjectPermission.objects.filter(user=self.user2).count(), 0)

        # User with share permission cannot revoke ``owner`` permission
        data = {'users': {'remove': {self.user2.pk: ['owner']}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(UserObjectPermission.objects.filter(user=self.user2).count(), 0)

        assign_perm("owner_collection", self.user1, self.collection)

        # User with share permission can grant ``owner`` permission
        data = {'users': {'add': {self.user2.pk: ['owner']}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.filter(user=self.user2).count(), 1)

        # User with share permission can revoke ``owner`` permission
        data = {'users': {'remove': {self.user2.pk: ['owner']}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.filter(user=self.user2).count(), 0)

    def test_share_content(self):
        data_1, data_2 = self._create_data(), self._create_data()
        entity_1, entity_2 = self._create_entity(), self._create_entity()

        self.collection.data.add(data_1, data_2)
        self.collection.entity_set.add(entity_1, entity_2)

        assign_perm("view_collection", self.user1, self.collection)
        assign_perm("share_collection", self.user1, self.collection)
        assign_perm("view_data", self.user1, data_1)
        assign_perm("view_data", self.user1, data_2)
        assign_perm("share_data", self.user1, data_1)
        assign_perm("view_entity", self.user1, entity_1)
        assign_perm("view_entity", self.user1, entity_2)
        assign_perm("share_entity", self.user1, entity_1)

        data = {'users': {'add': {self.user2.pk: ['view']}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.filter(user=self.user2).count(), 1)

        data = {'users': {'add': {self.user2.pk: ['view']}}, 'share_content': 'true'}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.filter(user=self.user2).count(), 3)
        self.assertTrue(UserObjectPermission.objects.filter(user=self.user2, object_pk=data_1.pk).exists())
        self.assertFalse(UserObjectPermission.objects.filter(user=self.user2, object_pk=data_2.pk).exists())
        self.assertTrue(UserObjectPermission.objects.filter(user=self.user2, object_pk=entity_1.pk).exists())
        self.assertFalse(UserObjectPermission.objects.filter(user=self.user2, object_pk=entity_2.pk).exists())

        data = {'users': {'remove': {self.user2.pk: ['view']}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.filter(user=self.user2).count(), 2)

        data = {'users': {'remove': {self.user2.pk: ['view']}}, 'share_content': 'true'}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.filter(user=self.user2).count(), 0)


class PermissionsUtilitiesTest(TestCase):

    def test_filter_owner_permission(self):
        """Check that ``owner`` permission is catched everywhere"""
        data_template = {
            'users': {
                'add': {
                    1: ['view'],
                    2: ['view', 'edit']
                },
                'remove': {
                    3: ['view', 'edit']
                }
            },
            'groups': {
                'add': {
                    1: ['view', 'edit']
                },
                'remove': {
                    2: ['view']
                }
            }
        }

        check_owner_permission(data_template)  # pylint: disable=protected-access

        data = deepcopy(data_template)
        data['users']['add'][1].append('owner')
        with self.assertRaises(exceptions.PermissionDenied):
            check_owner_permission(data)  # pylint: disable=protected-access

        data = deepcopy(data_template)
        data['users']['remove'][3].append('owner')
        with self.assertRaises(exceptions.PermissionDenied):
            check_owner_permission(data)  # pylint: disable=protected-access

        data = deepcopy(data_template)
        data['groups']['add'][1].append('owner')
        with self.assertRaises(exceptions.PermissionDenied):
            check_owner_permission(data)  # pylint: disable=protected-access

        data = deepcopy(data_template)
        data['groups']['remove'][2].append('owner')
        with self.assertRaises(exceptions.PermissionDenied):
            check_owner_permission(data)  # pylint: disable=protected-access

    def test_filter_user_permissions(self):
        """Check that user cannot change his own permissions"""
        data = {
            'users': {
                'add': {
                    1: ['view'],
                },
                'remove': {
                    2: ['view', 'edit']
                }
            }
        }

        with self.assertRaises(exceptions.PermissionDenied):
            check_user_permissions(data, 1)  # pylint: disable=protected-access

        with self.assertRaises(exceptions.PermissionDenied):
            check_user_permissions(data, 2)  # pylint: disable=protected-access

        check_user_permissions(data, 3)  # pylint: disable=protected-access

    def test_filter_public_permissions(self):
        """Check that public user cannot get to open permissions"""
        data = {'public': {'add': ['view']}}
        check_public_permissions(data)  # pylint: disable=protected-access

        data = {'public': {'add': ['download']}}
        check_public_permissions(data)  # pylint: disable=protected-access

        data = {'public': {'add': ['add']}}
        check_public_permissions(data)  # pylint: disable=protected-access

        data = {'public': {'add': ['edit']}}
        with self.assertRaises(exceptions.PermissionDenied):
            check_public_permissions(data)  # pylint: disable=protected-access

        data = {'public': {'add': ['share']}}
        with self.assertRaises(exceptions.PermissionDenied):
            check_public_permissions(data)  # pylint: disable=protected-access

        data = {'public': {'add': ['owner']}}
        with self.assertRaises(exceptions.PermissionDenied):
            check_public_permissions(data)  # pylint: disable=protected-access

        data = {'public': {'add': ['view', 'edit']}}
        with self.assertRaises(exceptions.PermissionDenied):
            check_public_permissions(data)  # pylint: disable=protected-access
