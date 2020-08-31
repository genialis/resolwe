# pylint: disable=missing-docstring
import shutil
import unittest
from datetime import timedelta

from django.utils.timezone import now

from guardian.shortcuts import remove_perm
from rest_framework import exceptions, status

from resolwe.flow.models import Collection, Data
from resolwe.flow.serializers import ContributorSerializer
from resolwe.flow.views import DataViewSet
from resolwe.test import ResolweAPITestCase

DATE_FORMAT = r"%Y-%m-%dT%H:%M:%S.%f"

MESSAGES = {
    "NOT_FOUND": "Not found.",
    # 'NO_PERMISSION': 'You do not have permission to perform this action.',
    "ONE_ID_REQUIRED": "Exactly one id required on create.",
}


class DataTestCase(ResolweAPITestCase):
    fixtures = [
        "users.yaml",
        "collections.yaml",
        "processes.yaml",
        "data.yaml",
        "permissions.yaml",
    ]

    def setUp(self):
        self.data1 = Data.objects.get(pk=1)

        self.resource_name = "data"
        self.viewset = DataViewSet

        self.data = {
            "name": "New data",
            "slug": "new_data",
            "collection": {"id": 1},
            "process": {"slug": "test_process"},
        }

        super().setUp()

    def tearDown(self):
        for data in Data.objects.all():
            if data.location:
                shutil.rmtree(data.location.get_path(), ignore_errors=True)

        super().tearDown()

    def test_get_list(self):
        resp = self._get_list(self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(len(resp.data), 2)

    def test_get_list_public_user(self):
        resp = self._get_list()
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(len(resp.data), 1)

    def test_get_list_admin(self):
        resp = self._get_list(self.admin)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(len(resp.data), 2)

    @unittest.skipIf(
        True,
        "since PR308: this test uses transactions, incompatible with the separated manager",
    )
    def test_post(self):
        # logged-in user w/ perms
        collection_n = Data.objects.count()

        resp = self._post(self.data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Data.objects.count(), collection_n + 1)

        d = Data.objects.get(pk=resp.data["id"])
        self.assertTrue(now() - d.modified < timedelta(seconds=1))
        self.assertTrue(now() - d.created < timedelta(seconds=1))
        self.assertEqual(d.status, "OK")

        self.assertTrue(now() - d.started < timedelta(seconds=1))
        self.assertTrue(now() - d.finished < timedelta(seconds=1))
        self.assertEqual(d.contributor_id, self.user1.pk)

    def test_post_invalid_fields(self):
        data_n = Data.objects.count()

        self.data["collection"] = {"id": 42}
        resp = self._post(self.data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            resp.data["collection"][0],
            "Invalid collection value: {'id': 42} - object does not exist.",
        )

        self.data["collection"] = {"id": 1}
        self.data["process"] = {"id": 42}
        resp = self._post(self.data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            str(resp.data["process"][0]),
            "Invalid process value: {'id': 42} - object does not exist.",
        )

        self.assertEqual(Data.objects.count(), data_n)

    def test_post_no_perms(self):
        collection = Collection.objects.get(pk=1)
        remove_perm("edit_collection", self.user2, collection)

        data_count = Data.objects.count()
        resp = self._post(self.data, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(Data.objects.count(), data_count)

    def test_post_public_user(self):
        data_count = Data.objects.count()
        resp = self._post(self.data)
        # User has no permission to add Data object to the collection.
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(Data.objects.count(), data_count)

    def test_post_protected_fields(self):
        date_now = now()
        self.data["created"] = date_now - timedelta(days=360)
        self.data["modified"] = date_now - timedelta(days=180)
        self.data["started"] = date_now - timedelta(days=180)
        self.data["finished"] = date_now - timedelta(days=90)
        self.data["checksum"] = "fake"
        self.data["status"] = "DE"
        self.data["process_progress"] = 2
        self.data["process_rc"] = 18
        self.data["process_info"] = "Spam"
        self.data["process_warning"] = "More spam"
        self.data["process_error"] = "Even more spam"
        self.data["contributor_id"] = 2

        resp = self._post(self.data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)

        self.assertEqual(resp.data["started"], None)
        self.assertEqual(resp.data["finished"], None)
        self.assertEqual(
            resp.data["checksum"],
            "05bc76611c382a88817389019679f35cdb32ac65fe6662210805b588c30f71e6",
        )
        self.assertEqual(resp.data["status"], "RE")
        self.assertEqual(resp.data["process_progress"], 0)
        self.assertEqual(resp.data["process_rc"], None)
        self.assertEqual(resp.data["process_info"], [])
        self.assertEqual(resp.data["process_warning"], [])
        self.assertEqual(resp.data["process_error"], [])
        self.assertEqual(
            resp.data["contributor"],
            {
                "id": self.user1.pk,
                "username": self.user1.username,
                "first_name": self.user1.first_name,
                "last_name": self.user1.last_name,
            },
        )

    def test_post_contributor_numeric(self):
        response = ContributorSerializer(
            ContributorSerializer().to_internal_value(self.user1.pk)
        ).data

        self.assertEqual(
            response,
            {
                "id": self.user1.pk,
                "username": self.user1.username,
                "first_name": self.user1.first_name,
                "last_name": self.user1.last_name,
            },
        )

    def test_post_contributor_dict(self):
        response = ContributorSerializer(
            ContributorSerializer().to_internal_value({"id": self.user1.pk})
        ).data

        self.assertEqual(
            response,
            {
                "id": self.user1.pk,
                "username": self.user1.username,
                "first_name": self.user1.first_name,
                "last_name": self.user1.last_name,
            },
        )

    def test_post_contributor_dict_extra_data(self):
        response = ContributorSerializer(
            ContributorSerializer().to_internal_value(
                {"id": self.user1.pk, "username": "ignored", "first_name": "ignored"}
            )
        ).data

        self.assertEqual(
            response,
            {
                "id": self.user1.pk,
                "username": self.user1.username,
                "first_name": self.user1.first_name,
                "last_name": self.user1.last_name,
            },
        )

    def test_post_contributor_dict_invalid(self):
        with self.assertRaises(exceptions.ValidationError):
            ContributorSerializer().to_internal_value(
                {
                    "invalid-dictionary": True,
                }
            )

    def test_get_detail(self):
        # public user w/ perms
        resp = self._get_detail(1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(resp.data["id"], 1)

        # user w/ permissions
        resp = self._get_detail(1, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertKeys(
            resp.data,
            [
                "slug",
                "name",
                "created",
                "modified",
                "contributor",
                "started",
                "finished",
                "checksum",
                "status",
                "process",
                "process_progress",
                "process_rc",
                "process_info",
                "process_warning",
                "process_error",
                "input",
                "output",
                "descriptor_schema",
                "descriptor",
                "id",
                "size",
                "scheduled",
                "current_user_permissions",
                "descriptor_dirty",
                "tags",
                "process_memory",
                "process_cores",
                "collection",
                "entity",
                "duplicated",
            ],
        )

        # user w/ public permissions
        resp = self._get_detail(1, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertKeys(
            resp.data,
            [
                "slug",
                "name",
                "created",
                "modified",
                "contributor",
                "started",
                "finished",
                "checksum",
                "status",
                "process",
                "process_progress",
                "process_rc",
                "process_info",
                "process_warning",
                "process_error",
                "input",
                "output",
                "descriptor_schema",
                "descriptor",
                "id",
                "size",
                "scheduled",
                "current_user_permissions",
                "descriptor_dirty",
                "tags",
                "process_memory",
                "process_cores",
                "collection",
                "entity",
                "duplicated",
            ],
        )

    def test_get_detail_no_perms(self):
        # public user w/o permissions
        resp = self._get_detail(2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(resp.data["detail"], MESSAGES["NOT_FOUND"])

        # user w/o permissions
        resp = self._get_detail(2, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(resp.data["detail"], MESSAGES["NOT_FOUND"])

    def test_patch(self):
        data = {"name": "New data"}
        resp = self._patch(1, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.name, "New data")

    def test_patch_no_perms(self):
        data = {"name": "New data"}
        resp = self._patch(2, data, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        d = Data.objects.get(pk=2)
        self.assertEqual(d.name, "Test data 2")

    def test_patch_public_user(self):
        data = {"name": "New data"}
        resp = self._patch(2, data)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        d = Data.objects.get(pk=2)
        self.assertEqual(d.name, "Test data 2")

    def test_patch_protected(self):
        date_now = now()

        # `created`
        resp = self._patch(1, {"created": date_now}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.created.isoformat(), self.data1.created.isoformat())
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `modified`
        resp = self._patch(1, {"modified": date_now - timedelta(days=180)}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `started`
        resp = self._patch(1, {"started": date_now}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.started.isoformat(), self.data1.started.isoformat())
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `finished`
        resp = self._patch(1, {"finished": date_now}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.finished.isoformat(), self.data1.finished.isoformat())
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `checksum`
        resp = self._patch(1, {"checksum": "fake"}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(
            d.checksum,
            "05bc76611c382a88817389019679f35cdb32ac65fe6662210805b588c30f71e6",
        )
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `status`
        resp = self._patch(1, {"status": "DE"}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.status, "OK")
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `process_progress`
        resp = self._patch(1, {"process_progress": 2}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.process_progress, 0)
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `process_rc`
        resp = self._patch(1, {"process_rc": 18}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.process_rc, None)
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `process_info`
        resp = self._patch(1, {"process_info": "Spam"}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.process_info, [])
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `process_warning`
        resp = self._patch(1, {"process_warning": "More spam"}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.process_warning, [])
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `process_error`
        resp = self._patch(1, {"process_error": "Even more spam"}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.process_error, [])
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `contributor`
        resp = self._patch(1, {"contributor": 2}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.contributor_id, 1)
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `process`
        resp = self._patch(1, {"process": 2}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.process_id, 1)
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

    def test_delete(self):
        resp = self._delete(1, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        query = Data.objects.filter(pk=1).exists()
        self.assertFalse(query)

    def test_delete_no_perms(self):
        resp = self._delete(2, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        query = Data.objects.filter(pk=2).exists()
        self.assertTrue(query)

    def test_delete_public_user(self):
        resp = self._delete(2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        query = Data.objects.filter(pk=2).exists()
        self.assertTrue(query)
