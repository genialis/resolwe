# pylint: disable=missing-docstring
from rest_framework import status

from resolwe.flow.models import Collection
from resolwe.flow.views import CollectionViewSet
from resolwe.test import ResolweAPITestCase

MESSAGES = {
    "NOT_FOUND": "Not found.",
    "NO_PERMISSION": "You do not have permission to perform this action.",
}


class CollectionTestCase(ResolweAPITestCase):
    fixtures = [
        "users.yaml",
        "data.yaml",
        "collections.yaml",
        "processes.yaml",
        "permissions.yaml",
    ]

    def setUp(self):
        self.collection1 = Collection.objects.get(pk=1)

        self.resource_name = "collection"
        self.viewset = CollectionViewSet

        self.post_data = {
            "name": "Test collection",
            "slug": "test_collection",
        }

        super().setUp()

    def test_get_list(self):
        resp = self._get_list(self.user1)
        self.assertEqual(len(resp.data), 3)

    def test_get_list_public_user(self):
        # public user
        resp = self._get_list()
        self.assertEqual(len(resp.data), 2)
        # check that you get right two objects
        self.assertEqual([p["id"] for p in resp.data], [1, 3])
        # check that (one of the) objects have expected keys
        self.assertKeys(
            resp.data[0],
            [
                "slug",
                "status",
                "name",
                "created",
                "modified",
                "contributor",
                "description",
                "id",
                "settings",
                "current_user_permissions",
                "data_count",
                "descriptor_schema",
                "descriptor",
                "descriptor_dirty",
                "tags",
                "duplicated",
                "entity_count",
            ],
        )

    def test_get_list_admin(self):
        resp = self._get_list(self.admin)
        self.assertEqual(len(resp.data), 3)

    def test_get_list_groups(self):
        resp = self._get_list(self.user3)
        self.assertEqual(len(resp.data), 3)

    def test_post(self):
        # logged-in user
        collection_n = Collection.objects.count()
        resp = self._post(self.post_data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Collection.objects.count(), collection_n + 1)

        # public user
        collection_n += 1
        resp = self._post(self.post_data)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(Collection.objects.count(), collection_n)

    def test_get_detail(self):
        # public user w/ perm
        resp = self._get_detail(1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(resp.data["id"], 1)

        # public user w/o perm
        resp = self._get_detail(2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(resp.data["detail"], MESSAGES["NOT_FOUND"])

        # user w/ permissions
        resp = self._get_detail(1, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertKeys(
            resp.data,
            [
                "slug",
                "status",
                "name",
                "created",
                "modified",
                "contributor",
                "description",
                "settings",
                "id",
                "current_user_permissions",
                "data_count",
                "descriptor_schema",
                "descriptor",
                "descriptor_dirty",
                "tags",
                "duplicated",
                "entity_count",
            ],
        )

        # user w/o permissions
        resp = self._get_detail(2, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(resp.data["detail"], MESSAGES["NOT_FOUND"])

        # user w/ public permissions
        resp = self._get_detail(1, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertKeys(
            resp.data,
            [
                "slug",
                "status",
                "name",
                "created",
                "modified",
                "contributor",
                "description",
                "settings",
                "id",
                "current_user_permissions",
                "data_count",
                "descriptor_schema",
                "descriptor",
                "descriptor_dirty",
                "tags",
                "duplicated",
                "entity_count",
            ],
        )

    def test_patch(self):
        data = {"name": "New collection"}
        resp = self._patch(1, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        p = Collection.objects.get(pk=1)
        self.assertEqual(p.name, "New collection")

        # protected field
        data = {"created": "3042-01-01T09:00:00"}
        resp = self._patch(1, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        p = Collection.objects.get(pk=1)
        self.assertEqual(p.created.isoformat(), self.collection1.created.isoformat())

    def test_patch_no_perm(self):
        data = {"name": "New collection"}
        resp = self._patch(2, data, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        p = Collection.objects.get(pk=2)
        self.assertEqual(p.name, "Test collection 2")

    def test_patch_public_user(self):
        data = {"name": "New collection"}
        resp = self._patch(3, data)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        p = Collection.objects.get(pk=3)
        self.assertEqual(p.name, "Test collection 3")

    def test_delete(self):
        resp = self._delete(1, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        collection_exists = Collection.objects.filter(pk=1).exists()
        self.assertFalse(collection_exists)

    def test_delete_no_perm(self):
        resp = self._delete(2, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        collection_exists = Collection.objects.filter(pk=2).exists()
        self.assertTrue(collection_exists)

    def test_delete_public_user(self):
        resp = self._delete(3)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        collection_exists = Collection.objects.filter(pk=3).exists()
        self.assertTrue(collection_exists)
