# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import time

from django.apps import apps
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.management import call_command
from django.test import override_settings

from guardian.shortcuts import assign_perm
from rest_framework.test import APITestCase, APIRequestFactory, force_authenticate

from resolwe.elastic.builder import index_builder
from resolwe.elastic.utils.tests import ElasticSearchTestCase


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

        apps.clear_cache()
        call_command('migrate', verbosity=0, interactive=False, load_initial_data=False)

        index_builder.indexes = [TestSearchIndex()]
        index_builder.register_signals()

        super(IndexViewsetTest, self).setUp()

        # Prepare users and groups
        user_model = get_user_model()
        self.user_1 = user_model.objects.create(username='user_one')
        self.user_2 = user_model.objects.create(username='user_two')
        group = Group.objects.create(name='group')
        group.user_set.add(self.user_2)

        # Prepare test data
        test_obj_1 = TestModel.objects.create(name='Object name 1', number=43)
        test_obj_2 = TestModel.objects.create(name='Object name 2', number=44)

        # Assing permissions
        assign_perm('view_model', self.user_1, test_obj_1)
        assign_perm('view_model', group, test_obj_2)
        self._wait_es()

        # Prepare test viewset
        self.test_viewset = TestViewSet.as_view(actions={
            'post': 'list_with_post',
        })

    def tearDown(self):
        super(IndexViewsetTest, self).tearDown()

        index_builder.unregister_signals()
        index_builder.indexes = []
        index_builder.destroy()

    def _wait_es(self):
        # TODO: Better solution for ES5:
        #       https://github.com/elastic/elasticsearch/pull/17986
        # wait for ElasticSearch to index the data
        time.sleep(2)

    def test_permissions(self):
        # First user
        request = factory.post('', {}, format='json')
        force_authenticate(request, self.user_1)
        response = self.test_viewset(request)

        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['name'], 'Object name 1')

        # Second user
        request = factory.post('', {}, format='json')
        force_authenticate(request, self.user_2)
        response = self.test_viewset(request)

        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['name'], 'Object name 2')

    def test_without_ordering(self):
        from .test_app.viewsets import TestEmptyOrderingViewSet

        viewset = TestEmptyOrderingViewSet.as_view(actions={
            'post': 'list_with_post',
        })

        request = factory.post('', {}, format='json')
        force_authenticate(request, self.user_1)
        response = viewset(request)

        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['name'], 'Object name 1')
