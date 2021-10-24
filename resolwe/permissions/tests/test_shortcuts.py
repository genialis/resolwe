# pylint: disable=missing-docstring,invalid-name
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group

from rest_framework import status
from rest_framework.test import APIRequestFactory, force_authenticate

from resolwe.flow.models import Collection, Data, Process
from resolwe.flow.views import StorageViewSet
from resolwe.permissions.models import Permission, get_anonymous_user
from resolwe.permissions.shortcuts import get_object_perms, get_user_group_perms
from resolwe.test import TestCase

factory = APIRequestFactory()


class UserGroupTestCase(TestCase):
    def setUp(self):
        super().setUp()

        self.group1 = Group.objects.create(name="Test group 1")
        self.group2 = Group.objects.create(name="Test group 2")

        self.collection = Collection.objects.create(
            contributor=self.contributor,
            name="Test collection",
        )

        # This collection is here to make sure that other permissions
        # don't affect tested queries.
        collection2 = Collection.objects.create(
            contributor=self.contributor,
            name="Test collection 2",
        )
        collection2.set_permission(Permission.VIEW, self.contributor)
        collection2.set_permission(Permission.VIEW, self.group1)

    def test_user(self):
        self.collection.set_permission(Permission.EDIT, self.contributor)

        user_perms, group_perms = get_user_group_perms(
            self.contributor, self.collection
        )

        self.assertEqual(len(group_perms), 0)
        self.assertCountEqual(user_perms, [Permission.VIEW, Permission.EDIT])

    def test_user_in_group(self):
        self.group1.user_set.add(self.contributor)
        self.collection.set_permission(Permission.EDIT, self.group1)

        user_perms, group_perms = get_user_group_perms(
            self.contributor, self.collection
        )
        self.assertEqual(len(group_perms), 1)
        self.assertCountEqual(group_perms[0][2], [Permission.VIEW, Permission.EDIT])
        self.assertEqual(len(user_perms), 0)

        self.collection.set_permission(Permission.VIEW, self.contributor)

        user_perms, group_perms = get_user_group_perms(
            self.contributor, self.collection
        )
        self.assertEqual(len(group_perms), 1)
        self.assertCountEqual(group_perms[0][2], [Permission.VIEW, Permission.EDIT])
        self.assertEqual(len(user_perms), 1)
        self.assertCountEqual(user_perms, [Permission.VIEW])

    def test_user_in_multiple_groups(self):
        self.group1.user_set.add(self.contributor)
        self.group2.user_set.add(self.contributor)
        self.collection.set_permission(Permission.EDIT, self.group1)
        self.collection.set_permission(Permission.VIEW, self.group2)

        user_perms, group_perms = get_user_group_perms(
            self.contributor, self.collection
        )
        self.assertEqual(len(group_perms), 2)
        self.assertEqual(group_perms[0][0], self.group1.pk)
        self.assertCountEqual(group_perms[0][2], [Permission.VIEW, Permission.EDIT])
        self.assertEqual(group_perms[1][0], self.group2.pk)
        self.assertCountEqual(group_perms[1][2], [Permission.VIEW])
        self.assertEqual(len(user_perms), 0)

    def test_group(self):
        self.collection.set_permission(Permission.EDIT, self.group1)
        user_perms, group_perms = get_user_group_perms(self.group1, self.collection)
        self.assertEqual(len(group_perms), 1)
        self.assertCountEqual(group_perms[0][2], [Permission.VIEW, Permission.EDIT])
        self.assertEqual(len(user_perms), 0)


class ObjectPermsTestCase(TestCase):
    def setUp(self):
        super().setUp()

        self.admin.delete()
        self.user1 = get_user_model().objects.create(username="test_user1")
        self.user2 = get_user_model().objects.create(username="test_user2")
        self.group1 = Group.objects.create(name="Test group 1")
        self.group2 = Group.objects.create(name="Test group 2")
        self.anonymous = get_anonymous_user()

        self.collection = Collection.objects.create(
            contributor=self.user1,
            name="Test collection",
        )

    def _sort_perms(self, perms):
        for elm in perms:
            elm["permissions"] = sorted(elm["permissions"])
        return perms

    def test_all_permissions(self):
        self.group1.user_set.add(self.user1)

        perms = get_object_perms(self.collection)
        self.assertEqual(len(perms), 0)

        self.collection.set_permission(Permission.EDIT, self.user1)
        self.collection.set_permission(Permission.VIEW, self.user2)
        expected_perms = [
            {
                "permissions": [Permission.EDIT, Permission.VIEW],
                "type": "user",
                "id": self.user1.pk,
                "name": "test_user1",
                "username": "test_user1",
            },
            {
                "permissions": [Permission.VIEW],
                "type": "user",
                "id": self.user2.pk,
                "name": "test_user2",
                "username": "test_user2",
            },
        ]
        perms = get_object_perms(self.collection)
        self.assertCountEqual(self._sort_perms(expected_perms), self._sort_perms(perms))

        self.collection.set_permission(Permission.EDIT, self.group1)
        self.collection.set_permission(Permission.VIEW, self.group2)
        expected_perms.extend(
            [
                {
                    "permissions": [Permission.EDIT, Permission.VIEW],
                    "type": "group",
                    "id": self.group1.pk,
                    "name": "Test group 1",
                },
                {
                    "permissions": [Permission.VIEW],
                    "type": "group",
                    "id": self.group2.pk,
                    "name": "Test group 2",
                },
            ]
        )
        perms = get_object_perms(self.collection)
        self.assertCountEqual(self._sort_perms(expected_perms), self._sort_perms(perms))

        self.collection.set_permission(Permission.VIEW, self.anonymous)
        expected_perms.append(
            {"permissions": [Permission.VIEW], "type": "public"},
        )
        perms = get_object_perms(self.collection)
        self.assertCountEqual(self._sort_perms(expected_perms), self._sort_perms(perms))

    def test_user_permissions(self):
        self.group1.user_set.add(self.user1)
        self.collection.set_permission(Permission.EDIT, self.user1)
        self.collection.set_permission(Permission.VIEW, self.user2)
        self.collection.set_permission(Permission.EDIT, self.group1)
        self.collection.set_permission(Permission.VIEW, self.group2)

        expected_perms = [
            {
                "permissions": [Permission.EDIT, Permission.VIEW],
                "type": "user",
                "id": self.user1.pk,
                "name": "test_user1",
                "username": "test_user1",
            },
            {
                "permissions": [Permission.EDIT, Permission.VIEW],
                "type": "group",
                "id": self.group1.pk,
                "name": "Test group 1",
            },
        ]
        perms = get_object_perms(self.collection, self.user1)

        self.assertCountEqual(self._sort_perms(expected_perms), self._sort_perms(perms))

        self.group2.user_set.add(self.user1)
        expected_perms.append(
            {
                "permissions": [Permission.VIEW],
                "type": "group",
                "id": self.group2.pk,
                "name": "Test group 2",
            },
        )
        perms = get_object_perms(self.collection, self.user1)
        self.assertCountEqual(self._sort_perms(expected_perms), self._sort_perms(perms))

        self.collection.set_permission(Permission.VIEW, self.anonymous)
        expected_perms.append(
            {"permissions": [Permission.VIEW], "type": "public"},
        )
        perms = get_object_perms(self.collection, self.user1)
        self.assertCountEqual(self._sort_perms(expected_perms), self._sort_perms(perms))


class StoragePermsTestCase(TestCase):
    def setUp(self):
        super().setUp()

        proc = Process.objects.create(name="Test process", contributor=self.contributor)
        self.data = Data.objects.create(
            name="Test data", contributor=self.contributor, process=proc
        )
        dummy_data = Data.objects.create(
            name="Dummy data", contributor=self.contributor, process=proc
        )

        self.storage1 = self.data.storages.create(
            name="Test storage",
            json={},
            contributor=self.contributor,
        )

        self.storage2 = self.data.storages.create(
            name="Test storage 2",
            json={},
            contributor=self.contributor,
        )

        dummy_data.storages.create(
            name="Dummy storage",
            json={},
            contributor=self.contributor,
        )

        self.user = get_user_model().objects.create(username="test_user")
        self.group = Group.objects.create(name="test_group")

        self.storage_list_viewset = StorageViewSet.as_view(
            actions={
                "get": "list",
            }
        )

        self.storage_detail_viewset = StorageViewSet.as_view(
            actions={
                "get": "retrieve",
            }
        )

    def test_detail_permissons(self):
        request = factory.get("/", content_type="application/json")
        force_authenticate(request, self.user)

        resp = self.storage_detail_viewset(request, pk=self.storage1.pk)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)

        self.data.set_permission(Permission.VIEW, self.user)
        resp = self.storage_detail_viewset(request, pk=self.storage1.pk)
        self.assertEqual(resp.data["name"], "Test storage")

        self.data.set_permission(None, self.user)
        resp = self.storage_detail_viewset(request, pk=self.storage1.pk)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)

    def test_not_allowed_methods(self):
        self.assertFalse(hasattr(StorageViewSet, "update"))
        self.assertFalse(hasattr(StorageViewSet, "partial_update"))
        self.assertFalse(hasattr(StorageViewSet, "destroy"))
        self.assertFalse(hasattr(StorageViewSet, "create"))
