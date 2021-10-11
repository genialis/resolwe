# pylint: disable=missing-docstring
from copy import deepcopy

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group

from rest_framework import exceptions, status

from resolwe.flow.models import Collection, DescriptorSchema, Process
from resolwe.flow.views import CollectionViewSet, DescriptorSchemaViewSet
from resolwe.permissions.models import Permission, PermissionModel
from resolwe.permissions.utils import (
    assign_contributor_permissions,
    check_owner_permission,
    check_public_permissions,
    check_user_permissions,
    get_anonymous_user,
    get_perms,
    set_permission,
)
from resolwe.test import ResolweAPITestCase, TestCase


class CollectionPermissionsTest(ResolweAPITestCase):
    def setUp(self):
        User = get_user_model()
        self.user1 = User.objects.create(username="test_user1", email="user1@test.com")
        self.user2 = User.objects.create(username="test_user2", email="user2@test.com")
        self.user3 = User.objects.create(username="test_user3", email="user1@test.com")
        self.owner = User.objects.create(username="owner")

        self.public = get_anonymous_user()
        self.group = Group.objects.create(name="Test group")

        self.collection = Collection.objects.create(
            contributor=self.owner, name="Test collection 1"
        )
        self.collection.set_permission("owner", self.owner)

        self.process = Process.objects.create(
            name="Test process",
            contributor=self.owner,
        )

        self.resource_name = "collection"
        self.viewset = CollectionViewSet

        super().setUp()

    def test_public_user(self):
        """Public user cannot create/edit anything"""
        set_permission("share", self.user1, self.collection)

        data = {"public": "view"}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)

        data = {"public": "edit"}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)

        data = {"public": "share"}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)

        data = {"public": "owner"}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)

    def test_user_without_share(self):
        """User without ``SHARE`` permission cannot do anything"""
        set_permission("edit", self.user1, self.collection)

        # Can not add permissions to users.
        data = {"users": {self.user2.pk: "view"}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)

        # Can not add permissions to groups.
        data = {"users": {self.group.pk: "view"}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)

    def test_user_with_share(self):
        self.collection.set_permission("share", self.user1)

        # Can set permissions to users.
        data = {"users": {self.user2.pk: "view"}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(get_perms(self.user2, self.collection), ["view"])

        # Can set permissions to groups.
        data = {"groups": {self.group.pk: "view"}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(get_perms(self.group, self.collection), ["view"])

    def test_protect_owner(self):
        """Only owners can modify `owner` permission"""
        self.collection.set_permission("share", self.user1)

        # User with share permission cannot grant ``owner`` permission
        data = {"users": {self.user2.pk: "owner"}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        self.assertNotIn("owner", get_perms(self.user2, self.collection))
        self.assertFalse(PermissionModel.objects.filter(users=self.user2).exists())

        # User with share permission cannot revoke ``owner`` permission
        data = {"users": {self.user2.pk: "owner"}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        self.assertFalse(PermissionModel.objects.filter(users=self.user2).exists())

        set_permission("owner", self.user1, self.collection)

        # ``owner`` permission cannot be assigned to a group
        data = {"groups": {self.group.pk: "owner"}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertFalse(PermissionModel.objects.filter(groups=self.group).exists())

        # User with owner permission can grant ``owner`` permission
        data = {"users": {self.user2.pk: "owner"}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(
            get_perms(self.user2, self.collection), ["view", "edit", "share", "owner"]
        )

        # User with owner permission can revoke ``owner`` permission
        data = {"users": {self.user2.pk: "edit"}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertFalse(
            PermissionModel.objects.filter(
                users=self.user2, permission=Permission.from_name("owner").value
            ).exists()
        )

        # User with owner permission cannot remove all owners
        data = {"users": {self.user1.pk: "edit", self.owner.pk: "edit"}}

        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(resp.data["detail"], "Object must have at least one owner.")

        owner_permission = self.collection.permission_group.permissions.get(
            permission=Permission.from_name("owner").value
        )
        owner_count = owner_permission.users.count()
        self.assertEqual(owner_count, 2)

        # User can delete his owner permission if there is at least one other owner
        self.assertTrue(owner_permission.users.filter(pk=self.user1.pk).exists())
        data = {"users": {self.user1.pk: "view"}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertFalse(owner_permission.users.filter(pk=self.user1.pk).exists())

    def test_nonexisting_permission(self):
        self.collection.set_permission("share", self.owner)

        # Add one valid permission to make sure that no permission is applied if any of them is unknown.
        data = {"users": {self.user1.pk: "view", self.user2.pk: "foo"}}
        resp = self._detail_permissions(self.collection.pk, data, self.owner)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(resp.data["detail"], "Unknown permission: foo")
        self.assertEqual(get_perms(self.user1, self.collection), [])
        self.assertEqual(get_perms(self.user2, self.collection), [])

    def test_nonexisting_user_group(self):
        self.collection.set_permission("share", self.owner)

        # Whole request should fail, so `user1` shouldn't have any permission assigned.
        data = {"users": {"999": "view", self.user1.pk: "view"}}
        resp = self._detail_permissions(self.collection.pk, data, self.owner)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(resp.data["detail"], "Unknown user: 999")
        # Maybe different?
        self.assertEqual(get_perms(self.user1, self.collection), [])

        # Whole request should fail, so `group` shouldn't have any permission assigned.
        data = {"groups": {"999": "view", self.group.pk: "view"}}
        resp = self._detail_permissions(self.collection.pk, data, self.owner)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(resp.data["detail"], "Unknown group: 999")
        self.assertEqual(get_perms(self.group, self.collection), [])

        self.collection.set_permission("view", self.user1)
        self.collection.set_permission("view", self.group)

    def test_share_by_email(self):
        set_permission("share", self.user1, self.collection)

        data = {"users": {self.user2.email: "view"}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(get_perms(self.user2, self.collection), ["view"])

        # Check if error is raise when trying to share with duplicated email.
        data = {"users": {self.user3.email: "view"}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            resp.data["detail"], "Cannot uniquely determine user: user1@test.com"
        )


class DescriptorSchemaPermissionsTest(ResolweAPITestCase):
    def setUp(self):
        self.group = Group.objects.create(name="Test group")

        self.resource_name = "collection"
        self.viewset = DescriptorSchemaViewSet

        super().setUp()

        self.descriptor_schema = DescriptorSchema.objects.create(
            contributor=self.contributor
        )
        assign_contributor_permissions(self.descriptor_schema)

    def test_set_permissions(self):
        # Can add permissions to users.
        data = {"users": {self.user.pk: "view"}}
        resp = self._detail_permissions(
            self.descriptor_schema.pk, data, self.contributor
        )
        self.assertEqual(resp.status_code, status.HTTP_200_OK)

        self.assertEqual(get_perms(self.user, self.descriptor_schema), ["view"])

        # Can add permissions to groups.
        data = {"groups": {self.group.pk: "view"}}
        resp = self._detail_permissions(
            self.descriptor_schema.pk, data, self.contributor
        )
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(get_perms(self.group, self.descriptor_schema), ["view"])

        # Can remove permissions from users.
        data = {"users": {self.user.pk: "NONE"}}
        resp = self._detail_permissions(
            self.descriptor_schema.pk, data, self.contributor
        )
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(get_perms(self.user, self.descriptor_schema), [])

        # Can remove permissions from groups.
        data = {"groups": {self.group.pk: "NONE"}}
        resp = self._detail_permissions(
            self.descriptor_schema.pk, data, self.contributor
        )
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(get_perms(self.group, self.descriptor_schema), [])


class PermissionsUtilitiesTest(TestCase):
    def test_filter_owner_permission(self):
        """Check that ``owner`` permission is catched everywhere"""
        data_template = {
            "users": {1: "view", 2: "edit", 3: "NONE"},
            "groups": {1: "edit", 2: "NONE"},
        }

        check_owner_permission(data_template, False)

        data = deepcopy(data_template)
        data["users"][1] = "owner"
        with self.assertRaises(exceptions.PermissionDenied):
            check_owner_permission(data, False)
        check_owner_permission(data, True)

        data = deepcopy(data_template)
        data["groups"][1] = "owner"
        with self.assertRaises(exceptions.ParseError):
            check_owner_permission(data, False)
        with self.assertRaises(exceptions.ParseError):
            check_owner_permission(data, True)

    def test_filter_user_permissions(self):
        """Check that user cannot change his own permissions"""
        data = {
            "users": {
                1: "view",
                2: "NONE",
            }
        }

        with self.assertRaises(exceptions.PermissionDenied):
            check_user_permissions(data, 1)

        with self.assertRaises(exceptions.PermissionDenied):
            check_user_permissions(data, 2)

        check_user_permissions(data, 3)

    def test_filter_public_permissions(self):
        """Check that public user cannot get to open permissions"""
        data = {"public": "view"}
        check_public_permissions(data)

        data = {"public": "edit"}
        with self.assertRaises(exceptions.PermissionDenied):
            check_public_permissions(data)

        data = {"public": "share"}
        with self.assertRaises(exceptions.PermissionDenied):
            check_public_permissions(data)

        data = {"public": "owner"}
        with self.assertRaises(exceptions.PermissionDenied):
            check_public_permissions(data)
