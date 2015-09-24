# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from datetime import datetime

from django.db import transaction

from rest_framework import status

from .base import ResolweAPITestCase
from resolwe.flow.models import Project
from resolwe.flow.views import ProjectViewSet


MESSAGES = {
    u'NOT_FOUND': u'Not found.',
    u'NO_PERMISSION': u'You do not have permission to perform this action.',
}


class ProjectTestCase(ResolweAPITestCase):
    fixtures = ['users.yaml', 'projects.yaml', 'permissions.yaml']

    def setUp(self):
        self.project1 = Project.objects.get(pk=1)

        self.resource_name = 'project'
        self.viewset = ProjectViewSet

        self.post_data = {
            'name': 'Test project',
            'slug': 'test_project',
        }

        super(ProjectTestCase, self).setUp()

    def test_get_list(self):
        resp = self._get_list(self.user1)
        self.assertEqual(len(resp.data), 2)
        # self.assertEqual(len(resp.data), 3)

    def test_get_list_public_user(self):
        # public user
        resp = self._get_list()
        self.assertEqual(len(resp.data), 2)
        # check that you get right two objects
        self.assertEqual([p['id'] for p in resp.data], [1, 3])
        # check that (one of the) objects have expected keys
        self.assertKeys(resp.data[0], [u'slug', u'name', u'created', u'modified', u'contributor',
                                       u'description', u'id', u'settings'])

    def test_get_list_admin(self):
        resp = self._get_list(self.admin)
        self.assertEqual(len(resp.data), 3)

    def test_get_list_groups(self):
        resp = self._get_list(self.user3)
        self.assertEqual(len(resp.data), 1)
        # self.assertEqual(len(resp.data), 3)

    def test_post(self):
        # logged-in user
        project_n = Project.objects.count()
        resp = self._post(self.post_data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Project.objects.count(), project_n + 1)

        # public user
        project_n += 1
        resp = self._post(self.post_data)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(Project.objects.count(), project_n)

    def test_get_detail(self):
        # public user w/ perm
        resp = self._get_detail(1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(resp.data[u'id'], 1)

        # public user w/o perm
        resp = self._get_detail(2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(resp.data[u'detail'], MESSAGES['NOT_FOUND'])

        # user w/ permissions
        resp = self._get_detail(1, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertKeys(resp.data, [u'slug', u'name', u'created', u'modified', u'contributor',
                                    u'description', u'settings', u'id'])

        # user w/o permissions
        resp = self._get_detail(2, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(resp.data[u'detail'], MESSAGES['NOT_FOUND'])

    def test_patch(self):
        data = {'name': 'New project'}
        resp = self._patch(1, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        p = Project.objects.get(pk=1)
        self.assertEqual(p.name, 'New project')

        # protected field
        data = {'created': '3042-01-01T09:00:00'}
        resp = self._patch(1, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        p = Project.objects.get(pk=1)
        self.assertEqual(p.created, datetime(2015, 1, 1, 9, 0, 0))

    def test_patch_no_perm(self):
        data = {'name': 'New project'}
        resp = self._patch(2, data, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        p = Project.objects.get(pk=2)
        self.assertEqual(p.name, 'Test project 2')

    def test_patch_public_user(self):
        data = {'name': 'New project'}
        resp = self._patch(3, data)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        p = Project.objects.get(pk=3)
        self.assertEqual(p.name, 'Test project 3')

    def test_delete(self):
        resp = self._delete(1, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        q = Project.objects.filter(pk=1).exists()
        self.assertFalse(q)

    def test_delete_no_perm(self):
        resp = self._delete(2, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        q = Project.objects.filter(pk=2).exists()
        self.assertTrue(q)

    def test_delete_public_user(self):
        resp = self._delete(3)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        q = Project.objects.filter(pk=3).exists()
        self.assertTrue(q)

    def test_post_duplicated_slug(self):
        # create new object
        Project.objects.create(contributor_id=1, **self.post_data)
        project_n = Project.objects.count()

        with transaction.atomic():
            resp = self._post(self.post_data, self.user1)
            self.assertEqual(resp.status_code, status.HTTP_409_CONFLICT)
            self.assertTrue(u'error' in resp.data)
        self.assertEqual(Project.objects.count(), project_n)
