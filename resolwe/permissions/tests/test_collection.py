# pylint: disable=missing-docstring
from django.contrib.auth import get_user_model
from django.test import TransactionTestCase as DjangoTransactionTestCase
from rest_framework import status

from resolwe.flow.models import Collection
from resolwe.flow.views import CollectionViewSet
from resolwe.observers.models import BackgroundTask
from resolwe.permissions.models import get_anonymous_user
from resolwe.test import ResolweAPITestCase, TransactionResolweAPITestCase

MESSAGES = {
    "NOT_FOUND": "No Collection matches the given query.",
    "NO_PERMISSION": "You do not have permission to perform this action.",
}


class CollectionTestCaseCommonMixin:
    fixtures = [
        "users.yaml",
        "permissions.yaml",
        "data.yaml",
        "collections.yaml",
        "processes.yaml",
    ]

    def setUp(self):
        self.collection1 = Collection.objects.get(pk=1)

        self.resource_name = "collection"
        self.viewset = CollectionViewSet

        self.post_data = {
            "name": "Test collection",
            "slug": "test_collection",
        }

        # Reset the anonymous user for each test. This is important since permissions
        # are set in fixtures but id of the public user changes between tests.
        get_anonymous_user(cache=False)
        super().setUp()


class CollectionTestCase(CollectionTestCaseCommonMixin, ResolweAPITestCase):
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


class CollectionTestCaseDelete(
    CollectionTestCaseCommonMixin,
    TransactionResolweAPITestCase,
    DjangoTransactionTestCase,
):
    """Test deleting collections.

    We have to inherit from DjangoTransactionTestCase because the deletion in done in a
    background task. The tests are in a separate class not to slow down the other tests.
    """

    def _pre_setup(self, *args, **kwargs):
        """Delete all previously created users.

        The public user is created in the migrations and recreated during test case,
        but with a different id which breaks the loading of the fixtures.
        """
        get_user_model().objects.all().delete()
        super()._pre_setup(*args, **kwargs)

    def test_delete(self):
        response = self._delete(1, self.user1)
        BackgroundTask.objects.get(pk=response.data["id"]).wait()
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        collection_exists = Collection.objects.filter(pk=1).exists()
        self.assertFalse(collection_exists)

    def test_delete_no_perm(self):
        response = self._delete(2, self.user2)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        collection_exists = Collection.objects.filter(pk=2).exists()
        self.assertTrue(collection_exists)

    def test_delete_public_user(self):
        response = self._delete(3)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        collection_exists = Collection.objects.filter(pk=3).exists()
        self.assertTrue(collection_exists)
