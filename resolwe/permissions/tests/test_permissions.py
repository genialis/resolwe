# pylint: disable=missing-docstring
from copy import deepcopy

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.contrib.contenttypes.models import ContentType

from guardian.models import GroupObjectPermission, UserObjectPermission
from guardian.shortcuts import assign_perm, get_perms
from rest_framework import exceptions, status

from resolwe.flow.models import Collection, Data, DescriptorSchema, Entity, Process
from resolwe.flow.views import CollectionViewSet, DescriptorSchemaViewSet
from resolwe.permissions.utils import (
    assign_contributor_permissions,
    check_owner_permission,
    check_public_permissions,
    check_user_permissions,
)
from resolwe.test import ResolweAPITestCase, TestCase


class CollectionPermissionsTest(ResolweAPITestCase):
    def setUp(self):
        self.user1 = get_user_model().objects.create(
            username="test_user1", email="user1@test.com"
        )
        self.user2 = get_user_model().objects.create(
            username="test_user2", email="user2@test.com"
        )
        self.user3 = get_user_model().objects.create(
            username="test_user3", email="user1@test.com"
        )
        self.owner = get_user_model().objects.create(username="owner")
        # public user is already created bt django-guardian
        self.public = get_user_model().objects.get(username="public")

        self.group = Group.objects.create(name="Test group")

        self.collection = Collection.objects.create(
            contributor=self.owner, name="Test collection 1"
        )
        assign_perm("owner_collection", self.owner, self.collection)

        self.process = Process.objects.create(
            name="Test process",
            contributor=self.owner,
        )

        self.resource_name = "collection"
        self.viewset = CollectionViewSet

        super().setUp()

    def test_public_user(self):
        """Public user cannot create/edit anything"""
        assign_perm("view_collection", self.user1, self.collection)
        assign_perm("share_collection", self.user1, self.collection)

        data = {"public": {"add": ["view"]}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)

        data = {"public": {"add": ["edit"]}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)

        data = {"public": {"remove": ["edit"]}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)

    def test_user_without_share(self):
        """User without ``SHARE`` permission cannot do anything"""
        assign_perm("view_collection", self.user1, self.collection)
        assign_perm("edit_collection", self.user1, self.collection)

        # Can not add permissions to users.
        data = {"users": {"add": {self.user2.pk: ["view"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)

        # Can not add permissions to groups.
        data = {"users": {"add": {self.group.pk: ["view"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)

        # Can not remove permissions from users.
        data = {"users": {"remove": {self.user2.pk: ["view"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)

        # Can not remove permissions from groups.
        data = {"users": {"remove": {self.group.pk: ["view"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)

    def test_user_with_share(self):
        assign_perm("view_collection", self.user1, self.collection)
        assign_perm("share_collection", self.user1, self.collection)

        # Can add permissions to users.
        data = {"users": {"add": {self.user2.pk: ["view"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(
            UserObjectPermission.objects.filter(user=self.user2).count(), 1
        )

        # Can add permissions to groups.
        data = {"groups": {"add": {self.group.pk: ["view"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(GroupObjectPermission.objects.count(), 1)

        # Can remove permissions from users.
        data = {"users": {"remove": {self.user2.pk: ["view"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(
            UserObjectPermission.objects.filter(user=self.user2).count(), 0
        )

        # Can remove permissions from groups.
        data = {"groups": {"remove": {self.group.pk: ["view"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(GroupObjectPermission.objects.count(), 0)

    def test_protect_owner(self):
        """Only owners can modify `owner` permission"""
        assign_perm("view_collection", self.user1, self.collection)
        assign_perm("edit_collection", self.user1, self.collection)
        assign_perm("share_collection", self.user1, self.collection)

        # User with share permission cannot grant ``owner`` permission
        data = {"users": {"add": {self.user2.pk: ["owner"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(
            UserObjectPermission.objects.filter(user=self.user2).count(), 0
        )

        # User with share permission cannot revoke ``owner`` permission
        data = {"users": {"remove": {self.user2.pk: ["owner"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(
            UserObjectPermission.objects.filter(user=self.user2).count(), 0
        )

        assign_perm("owner_collection", self.user1, self.collection)

        # ``owner`` permission cannot be assigned to a group
        data = {"groups": {"add": {self.group.pk: ["owner"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            GroupObjectPermission.objects.filter(group=self.group).count(), 0
        )

        # User with owner permission can grant ``owner`` permission
        data = {"users": {"add": {self.user2.pk: ["owner"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(
            UserObjectPermission.objects.filter(user=self.user2).count(), 1
        )

        # User with owner permission can revoke ``owner`` permission
        data = {"users": {"remove": {self.user2.pk: ["owner"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(
            UserObjectPermission.objects.filter(user=self.user2).count(), 0
        )

        # User with owner permission cannot remove all owners
        data = {
            "users": {"remove": {self.user1.pk: ["owner"], self.owner.pk: ["owner"]}}
        }
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(resp.data["detail"], "Object must have at least one owner.")

        owner_count = UserObjectPermission.objects.filter(
            object_pk=self.collection.pk,
            content_type=ContentType.objects.get_for_model(self.collection),
            permission__codename__startswith="owner_",
        ).count()
        self.assertEqual(owner_count, 2)

        # User can delete his owner permission if there is at least one other owner
        data = {"users": {"remove": {self.user1.pk: ["owner"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)

        user1_owner = UserObjectPermission.objects.filter(
            object_pk=self.collection.pk,
            content_type=ContentType.objects.get_for_model(self.collection),
            permission__codename__startswith="owner_",
            user=self.user1,
        ).exists()
        self.assertFalse(user1_owner)

    def test_share_content(self):
        data_1 = Data.objects.create(
            contributor=self.owner, process=self.process, name="Data 1"
        )
        data_2 = Data.objects.create(
            contributor=self.owner, process=self.process, name="Data 2"
        )
        entity_1 = Entity.objects.create(contributor=self.owner, name="Entity 1")
        entity_2 = Entity.objects.create(contributor=self.owner, name="Entity 2")

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

        data = {"users": {"add": {self.user2.pk: ["view"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(
            UserObjectPermission.objects.filter(user=self.user2).count(), 1
        )

        data = {"users": {"add": {self.user2.pk: ["view"]}}, "share_content": "true"}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(
            UserObjectPermission.objects.filter(user=self.user2).count(), 3
        )
        self.assertEqual(len(get_perms(self.user2, data_1)), 1)
        self.assertEqual(len(get_perms(self.user2, data_2)), 0)
        self.assertEqual(len(get_perms(self.user2, entity_1)), 1)
        self.assertEqual(len(get_perms(self.user2, entity_2)), 0)

        data = {"users": {"remove": {self.user2.pk: ["view"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(
            UserObjectPermission.objects.filter(user=self.user2).count(), 2
        )

        data = {"users": {"remove": {self.user2.pk: ["view"]}}, "share_content": "true"}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(
            UserObjectPermission.objects.filter(user=self.user2).count(), 0
        )

    def test_nonexisting_permission(self):
        assign_perm("view_collection", self.owner, self.collection)
        assign_perm("share_collection", self.owner, self.collection)

        # Add one valid permission to make sure that no permission is applied if any of them is unknown.
        data = {
            "users": {"add": {self.user1.pk: ["view"], self.user2.pk: ["view", "foo"]}}
        }
        resp = self._detail_permissions(self.collection.pk, data, self.owner)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(resp.data["detail"], "Unknown permission: foo")
        self.assertEqual(
            UserObjectPermission.objects.filter(user=self.user1).count(), 0
        )
        self.assertEqual(
            UserObjectPermission.objects.filter(user=self.user2).count(), 0
        )

        assign_perm("view_collection", self.user1, self.collection)
        assign_perm("view_collection", self.user2, self.collection)

        # Add one valid permission to make sure that no permission is applied if any of them is unknown.
        data = {
            "users": {
                "remove": {self.user1.pk: ["view"], self.user2.pk: ["view", "foo"]}
            }
        }
        resp = self._detail_permissions(self.collection.pk, data, self.owner)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(resp.data["detail"], "Unknown permission: foo")
        self.assertEqual(
            UserObjectPermission.objects.filter(user=self.user1).count(), 1
        )
        self.assertEqual(
            UserObjectPermission.objects.filter(user=self.user2).count(), 1
        )

    def test_nonexisting_user_group(self):
        assign_perm("view_collection", self.owner, self.collection)
        assign_perm("share_collection", self.owner, self.collection)

        user_perms_count = UserObjectPermission.objects.count()
        group_perms_count = GroupObjectPermission.objects.count()

        # Whole request should fail, so `user1` shouldn't have any permission assigned.
        data = {"users": {"add": {"999": ["view"], self.user1.pk: ["view"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.owner)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(resp.data["detail"], "Unknown user: 999")
        self.assertEqual(UserObjectPermission.objects.count(), user_perms_count)

        # Whole request should fail, so `group` shouldn't have any permission assigned.
        data = {"groups": {"add": {"999": ["view"], self.group.pk: ["view"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.owner)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(resp.data["detail"], "Unknown group: 999")
        self.assertEqual(GroupObjectPermission.objects.count(), group_perms_count)

        assign_perm("view_collection", self.user1, self.collection)
        assign_perm("view_collection", self.group, self.collection)

        user_perms_count = UserObjectPermission.objects.count()
        group_perms_count = GroupObjectPermission.objects.count()

        # Whole request should fail, so `user1` shouldn't have any permission removed.
        data = {"users": {"remove": {"999": ["view"], self.user1.pk: ["view"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.owner)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(resp.data["detail"], "Unknown user: 999")
        self.assertEqual(UserObjectPermission.objects.count(), user_perms_count)

        # Whole request should fail, so `group` shouldn't have any permission removed.
        data = {"groups": {"remove": {"999": ["view"], self.group.pk: ["view"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.owner)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(resp.data["detail"], "Unknown group: 999")
        self.assertEqual(GroupObjectPermission.objects.count(), group_perms_count)

    def test_share_by_email(self):
        assign_perm("view_collection", self.user1, self.collection)
        assign_perm("share_collection", self.user1, self.collection)

        data = {"users": {"add": {self.user2.email: ["view"]}}}
        resp = self._detail_permissions(self.collection.pk, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(
            UserObjectPermission.objects.filter(user=self.user2).count(), 1
        )

        # Check if error is raise when trying to share with duplicated email.
        data = {"users": {"add": {self.user3.email: ["view"]}}}
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
        data = {"users": {"add": {self.user.pk: ["view"]}}}
        resp = self._detail_permissions(
            self.descriptor_schema.pk, data, self.contributor
        )
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.filter(user=self.user).count(), 1)

        # Can add permissions to groups.
        data = {"groups": {"add": {self.group.pk: ["view"]}}}
        resp = self._detail_permissions(
            self.descriptor_schema.pk, data, self.contributor
        )
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(GroupObjectPermission.objects.count(), 1)

        # Can remove permissions from users.
        data = {"users": {"remove": {self.user.pk: ["view"]}}}
        resp = self._detail_permissions(
            self.descriptor_schema.pk, data, self.contributor
        )
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.filter(user=self.user).count(), 0)

        # Can remove permissions from groups.
        data = {"groups": {"remove": {self.group.pk: ["view"]}}}
        resp = self._detail_permissions(
            self.descriptor_schema.pk, data, self.contributor
        )
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(GroupObjectPermission.objects.count(), 0)


class PermissionsUtilitiesTest(TestCase):
    def test_filter_owner_permission(self):
        """Check that ``owner`` permission is catched everywhere"""
        data_template = {
            "users": {
                "add": {1: ["view"], 2: ["view", "edit"]},
                "remove": {3: ["view", "edit"]},
            },
            "groups": {"add": {1: ["view", "edit"]}, "remove": {2: ["view"]}},
        }

        check_owner_permission(data_template, False)

        data = deepcopy(data_template)
        data["users"]["add"][1].append("owner")
        with self.assertRaises(exceptions.PermissionDenied):
            check_owner_permission(data, False)
        check_owner_permission(data, True)

        data = deepcopy(data_template)
        data["users"]["remove"][3].append("owner")
        with self.assertRaises(exceptions.PermissionDenied):
            check_owner_permission(data, False)
        check_owner_permission(data, True)

        data = deepcopy(data_template)
        data["groups"]["add"][1].append("owner")
        with self.assertRaises(exceptions.ParseError):
            check_owner_permission(data, False)
        with self.assertRaises(exceptions.ParseError):
            check_owner_permission(data, True)

        data = deepcopy(data_template)
        data["groups"]["remove"][2].append("owner")
        with self.assertRaises(exceptions.ParseError):
            check_owner_permission(data, False)
        with self.assertRaises(exceptions.ParseError):
            check_owner_permission(data, True)

    def test_filter_user_permissions(self):
        """Check that user cannot change his own permissions"""
        data = {
            "users": {
                "add": {
                    1: ["view"],
                },
                "remove": {2: ["view", "edit"]},
            }
        }

        with self.assertRaises(exceptions.PermissionDenied):
            check_user_permissions(data, 1)

        with self.assertRaises(exceptions.PermissionDenied):
            check_user_permissions(data, 2)

        check_user_permissions(data, 3)

    def test_filter_public_permissions(self):
        """Check that public user cannot get to open permissions"""
        data = {"public": {"add": ["view"]}}
        check_public_permissions(data)

        data = {"public": {"add": ["add"]}}
        check_public_permissions(data)

        data = {"public": {"add": ["edit"]}}
        with self.assertRaises(exceptions.PermissionDenied):
            check_public_permissions(data)

        data = {"public": {"add": ["share"]}}
        with self.assertRaises(exceptions.PermissionDenied):
            check_public_permissions(data)

        data = {"public": {"add": ["owner"]}}
        with self.assertRaises(exceptions.PermissionDenied):
            check_public_permissions(data)

        data = {"public": {"add": ["view", "edit"]}}
        with self.assertRaises(exceptions.PermissionDenied):
            check_public_permissions(data)
