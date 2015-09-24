# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from guardian.models import GroupObjectPermission, UserObjectPermission

from rest_framework import status

from .base import ResolweAPITestCase
from resolwe.flow.models import Project
from resolwe.flow.views import ProjectViewSet


class PermissionsTestCase(ResolweAPITestCase):
    fixtures = ['users.yaml', 'projects.yaml', 'permissions.yaml']

    def setUp(self):
        self.project1 = Project.objects.get(pk=1)

        self.resource_name = 'project'
        self.viewset = ProjectViewSet

        super(PermissionsTestCase, self).setUp()

    def test_add_permissions(self):
        data = {
            'users': {
                'add': {
                    2: ['download']
                }
            },
            'groups': {
                'add': {
                    1: ['view', 'edit']
                }
            }
        }

        # add new permissions
        user_perms_n = UserObjectPermission.objects.count()
        group_perms_n = GroupObjectPermission.objects.count()
        resp = self._detail_permissions(1, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.count(), user_perms_n + 1)
        self.assertEqual(GroupObjectPermission.objects.count(), group_perms_n + 2)

        # add already existing permissions
        user_perms_n = UserObjectPermission.objects.count()
        group_perms_n = GroupObjectPermission.objects.count()
        resp = self._detail_permissions(1, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.count(), user_perms_n)
        self.assertEqual(GroupObjectPermission.objects.count(), group_perms_n)

        # invalid permissions
        data = {'users': {'add': {2: ['delete']}}}
        user_perms_n = UserObjectPermission.objects.count()
        resp = self._detail_permissions(1, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.count(), user_perms_n)

    def test_remove_permissions(self):
        data = {
            'users': {
                'remove': {
                    2: ['view', 'edit']
                }
            },
            'groups': {
                'remove': {
                    1: ['share']
                }
            }
        }

        # remove existing permissions
        user_perms_n = UserObjectPermission.objects.count()
        group_perms_n = GroupObjectPermission.objects.count()
        resp = self._detail_permissions(1, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.count(), user_perms_n - 2)
        self.assertEqual(GroupObjectPermission.objects.count(), group_perms_n - 1)

        # remove non-existing permissions
        user_perms_n = UserObjectPermission.objects.count()
        group_perms_n = GroupObjectPermission.objects.count()
        resp = self._detail_permissions(1, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.count(), user_perms_n)
        self.assertEqual(GroupObjectPermission.objects.count(), group_perms_n)

        # invalid permissions
        data = {'users': {'remove': {2: ['delete']}}}
        user_perms_n = UserObjectPermission.objects.count()
        resp = self._detail_permissions(1, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.count(), user_perms_n)

    def test_public_permissions(self):
        data = {
            'public': {
                'add': ['edit', 'share']
            },
        }
        user_perms_n = UserObjectPermission.objects.count()
        resp = self._detail_permissions(1, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.count(), user_perms_n)

        data = {
            'public': {
                'remove': ['view']
            },
        }
        user_perms_n = UserObjectPermission.objects.count()
        resp = self._detail_permissions(1, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(UserObjectPermission.objects.count(), user_perms_n - 1)

    def test_unauthorized_set(self):
        # registered user w/o perms
        user_perms_n = UserObjectPermission.objects.count()
        group_perms_n = GroupObjectPermission.objects.count()
        data = {'users': {'add': {2: ['download']}}}
        resp = self._detail_permissions(1, data, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(UserObjectPermission.objects.count(), user_perms_n)
        self.assertEqual(GroupObjectPermission.objects.count(), group_perms_n)

        # public user
        user_perms_n = UserObjectPermission.objects.count()
        group_perms_n = GroupObjectPermission.objects.count()
        resp = self._detail_permissions(1, {'users': {'add': {2: ['view', 'edit']}}})
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(UserObjectPermission.objects.count(), user_perms_n)
        self.assertEqual(GroupObjectPermission.objects.count(), group_perms_n)
