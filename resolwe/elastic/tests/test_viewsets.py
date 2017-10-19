# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import mock

from django.apps import apps
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser, Group
from django.core.management import call_command
from django.test import override_settings

from guardian.shortcuts import assign_perm
from rest_framework.test import APIRequestFactory, APITestCase, force_authenticate

from resolwe.elastic.builder import index_builder
from resolwe.test import ElasticSearchTestCase

factory = APIRequestFactory()  # pylint: disable=invalid-name


CUSTOM_SETTINGS = {
    'INSTALLED_APPS': settings.INSTALLED_APPS + ('resolwe.elastic.tests.test_app',),
}


@override_settings(**CUSTOM_SETTINGS)
class IndexViewsetTest(APITestCase, ElasticSearchTestCase):

    def setUp(self):
        from .test_app.models import TestModel
        from .test_app.elastic_indexes import TestSearchIndex
        from .test_app.viewsets import TestViewSet

        super(IndexViewsetTest, self).setUp()

        apps.clear_cache()
        call_command('migrate', verbosity=0, interactive=False, load_initial_data=False)

        index_builder.indexes = [TestSearchIndex()]
        index_builder.register_signals()

        # Prepare users and groups
        user_model = get_user_model()
        self.user_1 = user_model.objects.create(username='user_one')
        self.user_2 = user_model.objects.create(username='user_two')
        group = Group.objects.create(name='group')
        group.user_set.add(self.user_2)

        # Prepare test data
        test_obj_1 = TestModel.objects.create(name='Object name 1', number=43)
        test_obj_2 = TestModel.objects.create(name='Object name 2', number=44)
        test_obj_3 = TestModel.objects.create(name='Object name 3', number=45)

        # Assing permissions
        assign_perm('view_testmodel', self.user_1, test_obj_1)
        assign_perm('view_testmodel', group, test_obj_2)
        assign_perm('view_testmodel', AnonymousUser(), test_obj_3)

        # Prepare test viewset
        self.test_viewset = TestViewSet.as_view(actions={
            'post': 'list_with_post',
        })

    def tearDown(self):
        index_builder.destroy()
        super(IndexViewsetTest, self).tearDown()

    def test_permissions(self):
        # First user
        request = factory.post('', {}, format='json')
        force_authenticate(request, self.user_1)
        response = self.test_viewset(request)
        response = sorted(response.data, key=lambda obj: obj['name'])

        self.assertEqual(len(response), 2)
        self.assertEqual(response[0]['name'], 'Object name 1')
        self.assertEqual(response[1]['name'], 'Object name 3')

        # Second user
        request = factory.post('', {}, format='json')
        force_authenticate(request, self.user_2)
        response = self.test_viewset(request)
        response = sorted(response.data, key=lambda obj: obj['name'])

        self.assertEqual(len(response), 2)
        self.assertEqual(response[0]['name'], 'Object name 2')
        self.assertEqual(response[1]['name'], 'Object name 3')

        # Public user
        request = factory.post('', {}, format='json')
        response = self.test_viewset(request)

        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['name'], 'Object name 3')

    def test_without_ordering(self):
        from .test_app.viewsets import TestEmptyOrderingViewSet

        viewset = TestEmptyOrderingViewSet.as_view(actions={
            'post': 'list_with_post',
        })

        request = factory.post('', {}, format='json')
        force_authenticate(request, self.user_1)
        response = viewset(request)
        response = sorted(response.data, key=lambda obj: obj['name'])

        self.assertEqual(len(response), 2)
        self.assertEqual(response[0]['name'], 'Object name 1')
        self.assertEqual(response[1]['name'], 'Object name 3')

    def test_pagination(self):
        request = factory.post('', {'offset': '0', 'limit': '1'}, format='json')
        force_authenticate(request, self.user_1)
        response = self.test_viewset(request)

        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['name'], 'Object name 1')

        request = factory.post('', {'offset': '1', 'limit': '1'}, format='json')
        force_authenticate(request, self.user_1)
        response = self.test_viewset(request)

        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['name'], 'Object name 3')

    @mock.patch('resolwe.elastic.viewsets.ELASTICSEARCH_SIZE', 1)
    def test_pagination_elasticsearch_size_limit(self):  # pylint: disable=invalid-name
        request = factory.post('', {'offset': '0', 'limit': '1'}, format='json')
        force_authenticate(request, self.user_1)
        response = self.test_viewset(request)

        self.assertEqual(len(response.data['results']), 1)

    def test_custom_filter(self):
        from .test_app.viewsets import TestCustomFieldFilterViewSet

        viewset = TestCustomFieldFilterViewSet.as_view(actions={
            'post': 'list_with_post',
        })

        request = factory.post('', {'name': '1'}, format='json')
        force_authenticate(request, self.user_1)
        response = viewset(request)

        self.assertEqual(len(response.data), 0)

        request = factory.post('', {'name': '43'}, format='json')
        force_authenticate(request, self.user_1)
        response = viewset(request)

        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['name'], 'Object name 1')

    def test_combined_viewset(self):
        from .test_app.viewsets import TestCombinedViewSet

        viewset = TestCombinedViewSet.as_view(actions={
            'get': 'list'
        })

        # Test database-only access.
        request = factory.get('', {}, format='json')
        force_authenticate(request, self.user_1)
        response = viewset(request)

        self.assertEqual(len(response.data), 2)
        self.assertEqual(response.data[0]['name'], 'Object name 1')
        self.assertEqual(response.data[0]['field_process_type'], '')
        self.assertEqual(response.data[0]['number'], 43)
        self.assertEqual(response.data[1]['name'], 'Object name 3')

        # Test combined access.
        request = factory.get('', {'name': '43'}, format='json')
        force_authenticate(request, self.user_1)
        response = viewset(request)

        self.assertEqual(len(response.data), 0)

        request = factory.get('', {'name': '1'}, format='json')
        force_authenticate(request, self.user_1)
        response = viewset(request)

        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['name'], 'Object name 1')
        self.assertEqual(response.data[0]['field_process_type'], '')
        self.assertEqual(response.data[0]['number'], 43)

        # Test combined order.
        request = factory.get('', {'name': 'Object', 'ordering': 'name'})
        force_authenticate(request, self.user_1)
        response = viewset(request)

        self.assertEqual(len(response.data), 2)
        self.assertEqual(response.data[0]['name'], 'Object name 1')
        self.assertEqual(response.data[1]['name'], 'Object name 3')

        request = factory.get('', {'name': 'Object', 'ordering': '-name'})
        force_authenticate(request, self.user_1)
        response = viewset(request)

        self.assertEqual(len(response.data), 2)
        self.assertEqual(response.data[0]['name'], 'Object name 3')
        self.assertEqual(response.data[1]['name'], 'Object name 1')
