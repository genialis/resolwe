# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import os
import shutil

from datetime import datetime, timedelta


from django.conf import settings
if settings.USE_TZ:
    from django.utils.timezone import now
else:
    now = datetime.now  # pylint: disable=invalid-name

from rest_framework import status

from .base import ResolweAPITestCase
from resolwe.flow.models import Data, Collection
from resolwe.flow.views import DataViewSet


DATE_FORMAT = r'%Y-%m-%dT%H:%M:%S.%f'

MESSAGES = {
    u'NOT_FOUND': u'Not found.',
    # u'NO_PERMISSION': u'You do not have permission to perform this action.',
    u'ONE_ID_REQUIRED': 'Exactly one id required on create.',
}


class DataTestCase(ResolweAPITestCase):
    fixtures = ['users.yaml', 'collections.yaml', 'permissions.yaml', 'processes.yaml', 'data.yaml']

    def setUp(self):
        self.data1 = Data.objects.get(pk=1)

        self.resource_name = 'data'
        self.viewset = DataViewSet

        self.data = {
            'name': 'New data',
            'slug': 'new_data',
            'collections': ['1'],
            'process': 'test_process',
        }

        super(DataTestCase, self).setUp()

    def tearDown(self):
        for data in Data.objects.all():
            data_dir = os.path.join(settings.FLOW_EXECUTOR['DATA_PATH'], str(data.id))
            shutil.rmtree(data_dir, ignore_errors=True)

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
        collection_n = Data.objects.count()

        resp = self._post(self.data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Data.objects.count(), collection_n + 1)

        d = Data.objects.get(pk=resp.data['id'])
        self.assertTrue(now() - d.modified < timedelta(seconds=1))
        self.assertTrue(now() - d.created < timedelta(seconds=1))
        self.assertEqual(d.status, 'OK')

        self.assertTrue(now() - d.started < timedelta(seconds=1))
        self.assertTrue(now() - d.finished < timedelta(seconds=1))
        self.assertEqual(d.contributor_id, 1)

    def test_post_invalid_fields(self):
        data_n = Data.objects.count()

        self.data['collections'] = ['42']
        resp = self._post(self.data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(resp.data[u'collections'][0], u'Invalid pk "42" - object does not exist.')

        self.data['collections'] = ['1']
        self.data['process'] = '42'
        resp = self._post(self.data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(resp.data[u'process'][0], u'Invalid process slug "42" - object does not exist.')

        self.assertEqual(Data.objects.count(), data_n)

    def test_post_no_perms(self):
        collection_n = Data.objects.count()
        resp = self._post(self.data, self.user2)
        self.assertEqual(resp.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual(Data.objects.count(), collection_n)

    def test_post_public_user(self):
        collection_n = Data.objects.count()
        resp = self._post(self.data)
        self.assertEqual(resp.status_code, status.HTTP_404_NOT_FOUND)
        self.assertEqual(Data.objects.count(), collection_n)

    def test_post_protected_fields(self):
        date_now = now()
        self.data['created'] = date_now - timedelta(days=360)
        self.data['modified'] = date_now - timedelta(days=180)
        self.data['started'] = date_now - timedelta(days=180)
        self.data['finished'] = date_now - timedelta(days=90)
        self.data['checksum'] = 'fake'
        self.data['status'] = 'DE'
        self.data['process_progress'] = 2
        self.data['process_rc'] = 18
        self.data['process_info'] = 'Spam'
        self.data['process_warning'] = 'More spam'
        self.data['process_error'] = 'Even more spam'
        self.data['contributor_id'] = 2

        resp = self._post(self.data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)

        self.assertEqual(resp.data['started'], None)
        self.assertEqual(resp.data['finished'], None)
        self.assertEqual(resp.data['checksum'], None)  # TODO: Add checksum when implemented
        self.assertEqual(resp.data['status'], 'RE')
        self.assertEqual(resp.data['process_progress'], 0)
        self.assertEqual(resp.data['process_rc'], None)
        self.assertEqual(resp.data['process_info'], [])
        self.assertEqual(resp.data['process_warning'], [])
        self.assertEqual(resp.data['process_error'], [])
        self.assertEqual(resp.data['contributor'], 1)

    def test_post_multiple_collections(self):
        self.data['collections'].append('2')

        resp = self._post(self.data, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_201_CREATED)
        self.assertEqual(Collection.objects.filter(data=resp.data['id']).count(), 2)

    def test_get_detail(self):
        # public user w/ perms
        resp = self._get_detail(1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertEqual(resp.data[u'id'], 1)

        # user w/ permissions
        resp = self._get_detail(1, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_200_OK)
        self.assertKeys(resp.data, [u'slug', u'name', u'created', u'modified', u'contributor',
                                    u'started', u'finished', u'checksum', u'status', u'process',
                                    u'process_progress', u'process_rc', u'process_info', u'process_warning',
                                    u'process_error', u'input', u'output', u'process_type', u'descriptor_schema',
                                    u'descriptor', u'id', u'process_name', u'process_input_schema',
                                    u'process_output_schema', u'permissions'])

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
        date_now = now()

        # `created`
        resp = self._patch(1, {'created': date_now}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.created.isoformat(), self.data1.created.isoformat())
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `modified`
        resp = self._patch(1, {'modified': date_now - timedelta(days=180)}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `started`
        resp = self._patch(1, {'started': date_now}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.started.isoformat(), self.data1.started.isoformat())
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `finished`
        resp = self._patch(1, {'finished': date_now}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.finished.isoformat(), self.data1.finished.isoformat())
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `checksum`
        resp = self._patch(1, {'checksum': 'fake'}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.checksum, None)  # TODO: Add checksum when implemented
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `status`
        resp = self._patch(1, {'status': 'DE'}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.status, 'OK')
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `process_progress`
        resp = self._patch(1, {'process_progress': 2}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.process_progress, 0)
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `process_rc`
        resp = self._patch(1, {'process_rc': 18}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.process_rc, None)
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `process_info`
        resp = self._patch(1, {'process_info': 'Spam'}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.process_info, [])
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `process_warning`
        resp = self._patch(1, {'process_warning': 'More spam'}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.process_warning, [])
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `process_error`
        resp = self._patch(1, {'process_error': 'Even more spam'}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.process_error, [])
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `contributor`
        resp = self._patch(1, {'contributor': 2}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.contributor_id, 1)
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

        # `process`
        resp = self._patch(1, {'process': 2}, self.user1)
        self.assertEqual(resp.status_code, status.HTTP_204_NO_CONTENT)
        d = Data.objects.get(pk=1)
        self.assertEqual(d.process_id, 1)
        self.assertEqual(d.modified.isoformat(), self.data1.modified.isoformat())

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
