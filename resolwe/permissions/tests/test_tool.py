# pylint: disable=missing-docstring
from rest_framework import status

from resolwe.flow.models import Process
from resolwe.flow.views import ProcessViewSet
from resolwe.test import ResolweAPITestCase


class ProcessTestCase(ResolweAPITestCase):
    fixtures = [
        "users.yaml",
        "permissions.yaml",
        "processes.yaml",
        "data.yaml",
        "collections.yaml",
    ]

    def setUp(self):
        self.process1 = Process.objects.get(pk=1)

        self.post_data = {
            "slug": "new-process",
            "name": "New process",
            "type": "data:test:process:",
            "input_schema": [{"name": "test_field"}],
            "run": {"bash": "echo $PATH"},
        }

        self.resource_name = "process"
        self.viewset = ProcessViewSet

        super().setUp()

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

    def test_post(self):
        resp = self._post(self.post_data, self.admin)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)

    def test_patch(self):
        resp = self._patch(1, {"name": "Hacked process"}, self.admin)
        self.assertEqual(resp.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)

    def test_delete(self):
        resp = self._delete(1, self.admin)
        self.assertEqual(resp.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)
