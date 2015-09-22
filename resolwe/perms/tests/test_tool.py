# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from rest_framework import status

from .base import ResolweAPITestCase
from resolwe.flow.models import Project, Process
from resolwe.flow.views import ProcessViewSet


class ProcessTestCase(ResolweAPITestCase):
    fixtures = ['users.yaml', 'permissions.yaml', 'processes.yaml', 'projects.yaml']

    def setUp(self):
        self.process1 = Process.objects.get(pk=1)

        self.resource_name = 'process'
        self.viewset = ProcessViewSet

        super(ProcessTestCase, self).setUp()

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
        resp = self._post({'name': 'New process'}, self.admin)
        self.assertEqual(resp.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)

    def test_patch(self):
        resp = self._patch(1, {'name': 'Hacked process'}, self.admin)
        self.assertEqual(resp.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)

    def test_delete(self):
        resp = self._delete(1, self.admin)
        self.assertEqual(resp.status_code, status.HTTP_405_METHOD_NOT_ALLOWED)

    def test_project_processes(self):
        data = {'projects': {'add': ['1']}}

        # add new `process` to `project`
        self.assertFalse(Project.objects.get(pk=1).public_processes.filter(pk=self.process1.pk).exists())
        resp = self._detail_permissions(1, data, self.user1)
        self.assertEqual(resp.status_code, 200)
        self.process1.refresh_from_db()
        self.assertTrue(Project.objects.get(pk=1).public_processes.filter(pk=self.process1.pk).exists())

        data = {'projects': {'remove': ['1']}}

        # remove existing `process` from `project`
        resp = self._detail_permissions(1, data, self.user1)
        self.assertEqual(resp.status_code, 200)
        self.process1.refresh_from_db()
        self.assertFalse(Project.objects.get(pk=1).public_processes.filter(pk=self.process1.pk).exists())

        # remove non-existent `process` from `project`
        resp = self._detail_permissions(1, data, self.user1)
        self.assertEqual(resp.status_code, 200)

        data = {'projects': {'add': ['42', '1']}}

        # combination of valid and invalid `project`s
        self.assertFalse(Project.objects.get(pk=1).public_processes.filter(pk=self.process1.pk).exists())
        resp = self._detail_permissions(1, data, self.user1)
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(Project.objects.get(pk=1).public_processes.filter(pk=self.process1.pk).exists())
