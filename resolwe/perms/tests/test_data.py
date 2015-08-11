# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

from datetime import datetime, timedelta

from rest_framework import status

from .base import ResolweAPITestCase
from resolwe.flow.models import Data
from resolwe.flow.views import DataViewSet


DATE_FORMAT = r'%Y-%m-%dT%H:%M:%S.%f'

MESSAGES = {
    u'NOT_FOUND': u'Not found.',
    # u'NO_PERMISSION': u'You do not have permission to perform this action.',
}


class DataTestCase(ResolweAPITestCase):
    fixtures = ['users.yaml', 'projects.yaml', 'permissions.yaml', 'tools.yaml', 'data.yaml']

    def setUp(self):
        # self.data1 = Data.objects.get(pk=1)

        self.resource_name = 'data'
        self.viewset = DataViewSet

        self.data = {
            'name': 'New data',
            'slug': 'new_data',
            'projects': ['1'],
            'tool': '1',
        }

        super(DataTestCase, self).setUp()

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
        # logged-in user w/ perms
        project_n = Data.objects.count()
        resp = self._post(self.data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Data.objects.count(), project_n + 1)

        d = Data.objects.get(pk=resp.data['id'])
        self.assertTrue(datetime.now() - d.modified < timedelta(seconds=1))
        self.assertTrue(datetime.now() - d.created < timedelta(seconds=1))
        self.assertEqual(d.status, 'RE')
        self.assertEqual(d.started, None)
        self.assertEqual(d.finished, None)
        self.assertEqual(d.contributor_id, 1)

    def test_post_invalid_fields(self):
        data_n = Data.objects.count()

        self.data['projects'] = ['42']
        resp = self._post(self.data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(resp.data[u'projects'][0], u'Invalid pk "42" - object does not exist.')

        self.data['projects'] = ['1']
        self.data['tool'] = '42'
        resp = self._post(self.data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(resp.data[u'tool'][0], u'Invalid pk "42" - object does not exist.')

        self.assertEqual(Data.objects.count(), data_n)

    def test_post_no_perms(self):
        project_n = Data.objects.count()
        resp = self._post(self.data, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(Data.objects.count(), project_n)

    def test_post_public_user(self):
        project_n = Data.objects.count()
        resp = self._post(self.data)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(Data.objects.count(), project_n)

    def test_post_protected_fields(self):
        now = datetime.now()
        self.data['created'] = now - timedelta(days=360)
        self.data['modified'] = now - timedelta(days=180)
        self.data['started'] = now - timedelta(days=180)
        self.data['finished'] = now - timedelta(days=90)
        self.data['checksum'] = 'fake'
        self.data['status'] = 'DE'
        self.data['tool_progress'] = 2
        self.data['tool_rc'] = 18
        self.data['tool_info'] = 'Spam'
        self.data['tool_warning'] = 'More spam'
        self.data['tool_error'] = 'Even more spam'
        self.data['contributor_id'] = 2

        resp = self._post(self.data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)
        now = datetime.now()
        modified = datetime.strptime(resp.data['modified'], DATE_FORMAT)
        created = datetime.strptime(resp.data['created'], DATE_FORMAT)

        self.assertTrue(now - modified < timedelta(seconds=1))
        self.assertTrue(now - created < timedelta(seconds=1))
        self.assertEqual(resp.data['started'], None)
        self.assertEqual(resp.data['finished'], None)
        self.assertEqual(resp.data['checksum'], None)  # TODO: Add checksum when implemented
        self.assertEqual(resp.data['status'], 'RE')
        self.assertEqual(resp.data['tool_progress'], 0)
        self.assertEqual(resp.data['tool_rc'], None)
        self.assertEqual(resp.data['tool_info'], [])
        self.assertEqual(resp.data['tool_warning'], [])
        self.assertEqual(resp.data['tool_error'], [])
        self.assertEqual(resp.data['contributor'], 1)

    def test_get_detail(self):
        # public user w/ perms
        resp = self._get_detail(1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(resp.data[u'id'], 1)

        # user w/ permissions
        resp = self._get_detail(1, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertKeys(resp.data, [u'slug', u'name', u'created', u'modified', u'contributor',
                                    u'started', u'finished', u'checksum', u'status', u'tool',
                                    u'tool_progress', u'tool_rc', u'tool_info', u'tool_warning',
                                    u'tool_error', u'input', u'output', u'annotation_schema',
                                    u'annotation', u'id'])

    def test_get_detail_no_perms(self):
        # public user w/o permissions
        resp = self._get_detail(2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(resp.data[u'detail'], MESSAGES['NOT_FOUND'])

        # user w/o permissions
        resp = self._get_detail(2, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(resp.data[u'detail'], MESSAGES['NOT_FOUND'])

    def test_patch(self):
        data = {'name': 'New data'}
        resp = self._patch(1, data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.name, 'New data')

    def test_patch_no_perms(self):
        data = {'name': 'New data'}
        resp = self._patch(2, data, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        d = Data.objects.get(pk=2)
        self.assertEqual(d.name, 'Test data 2')

    def test_patch_public_user(self):
        data = {'name': 'New data'}
        resp = self._patch(2, data)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        d = Data.objects.get(pk=2)
        self.assertEqual(d.name, 'Test data 2')

    def test_patch_protected(self):
        now = datetime.now()

        # `created`
        resp = self._patch(1, {'created': now}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.created, datetime(2015, 1, 1, 9, 0, 0))
        self.assertEqual(d.modified, datetime(2015, 1, 1, 9, 0, 0))

        # `modified`
        resp = self._patch(1, {'modified': now - timedelta(days=180)}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.modified, datetime(2015, 1, 1, 9, 0, 0))

        # `started`
        resp = self._patch(1, {'started': now}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.started, datetime(2015, 1, 1, 9, 0, 0))
        self.assertEqual(d.modified, datetime(2015, 1, 1, 9, 0, 0))

        # `finished`
        resp = self._patch(1, {'finished': now}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.finished, datetime(2015, 1, 1, 9, 0, 0))
        self.assertEqual(d.modified, datetime(2015, 1, 1, 9, 0, 0))

        # `checksum`
        resp = self._patch(1, {'checksum': 'fake'}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.checksum, None)  # TODO: Add checksum when implemented
        self.assertEqual(d.modified, datetime(2015, 1, 1, 9, 0, 0))

        # `status`
        resp = self._patch(1, {'status': 'DE'}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.status, 'OK')
        self.assertEqual(d.modified, datetime(2015, 1, 1, 9, 0, 0))

        # `tool_progress`
        resp = self._patch(1, {'tool_progress': 2}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.tool_progress, 0)
        self.assertEqual(d.modified, datetime(2015, 1, 1, 9, 0, 0))

        # `tool_rc`
        resp = self._patch(1, {'tool_rc': 18}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.tool_rc, None)
        self.assertEqual(d.modified, datetime(2015, 1, 1, 9, 0, 0))

        # `tool_info`
        resp = self._patch(1, {'tool_info': 'Spam'}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.tool_info, [])
        self.assertEqual(d.modified, datetime(2015, 1, 1, 9, 0, 0))

        # `tool_warning`
        resp = self._patch(1, {'tool_warning': 'More spam'}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.tool_warning, [])
        self.assertEqual(d.modified, datetime(2015, 1, 1, 9, 0, 0))

        # `tool_error`
        resp = self._patch(1, {'tool_error': 'Even more spam'}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.tool_error, [])
        self.assertEqual(d.modified, datetime(2015, 1, 1, 9, 0, 0))

        # `contributor`
        resp = self._patch(1, {'contributor': 2}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.contributor_id, 1)
        self.assertEqual(d.modified, datetime(2015, 1, 1, 9, 0, 0))

        # `tool`
        resp = self._patch(1, {'tool': 2}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.tool_id, 1)
        self.assertEqual(d.modified, datetime(2015, 1, 1, 9, 0, 0))

    def test_delete(self):
        resp = self._delete(1, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        q = Data.objects.filter(pk=1).exists()
        self.assertFalse(q)

    def test_delete_no_perms(self):
        resp = self._delete(2, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        q = Data.objects.filter(pk=2).exists()
        self.assertTrue(q)

    def test_delete_public_user(self):
        resp = self._delete(2)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        q = Data.objects.filter(pk=2).exists()
        self.assertTrue(q)
