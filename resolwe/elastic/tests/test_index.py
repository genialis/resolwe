# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import time

from django.apps import apps
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.management import call_command
from django.test import override_settings

from guardian.shortcuts import assign_perm, remove_perm

from resolwe.elastic.builder import index_builder
from resolwe.elastic.utils.tests import ElasticSearchTestCase

CUSTOM_SETTINGS = {
    'INSTALLED_APPS': settings.INSTALLED_APPS + ('resolwe.elastic.tests.test_app',),
}


@override_settings(**CUSTOM_SETTINGS)
class IndexTest(ElasticSearchTestCase):
    def setUp(self):
        from .test_app.elastic_indexes import TestSearchIndex, TestAnalyzerSearchIndex

        apps.clear_cache()
        call_command('migrate', verbosity=0, interactive=False, load_initial_data=False)

        index_builder.indexes = [
            TestSearchIndex(),
            TestAnalyzerSearchIndex(),
        ]
        index_builder.register_signals()

        super(IndexTest, self).setUp()

    def tearDown(self):
        super(IndexTest, self).tearDown()

        index_builder.unregister_signals()
        index_builder.indexes = []
        index_builder.destroy()

    def _wait_es(self):
        # TODO: Better solution for ES5:
        #       https://github.com/elastic/elasticsearch/pull/17986
        # wait for ElasticSearch to index the data
        time.sleep(2)

    def test_mapping_multiple_times(self):
        index_builder.create_mappings()
        index_builder.create_mappings()

    def test_indexing(self):
        from .test_app.models import TestModel
        from .test_app.elastic_indexes import TestSearchDocument, TestSearchIndex

        # Create new object
        test_obj = TestModel.objects.create(name='Object name', number=43)
        self._wait_es()

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 1)
        self.assertEqual(es_objects[0].name, 'Object name')
        self.assertEqual(es_objects[0].num, 43)
        self.assertEqual(es_objects[0].json['key'], 'value')

        # Update existing object
        test_obj.name = 'Another name'
        test_obj.save()
        self._wait_es()

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 1)
        self.assertEqual(es_objects[0].name, 'Another name')
        self.assertEqual(es_objects[0].num, 43)
        self.assertEqual(es_objects[0].json['key'], 'value')

        # Create another object
        TestModel.objects.create(name='Another object', number=3)
        self._wait_es()

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 2)

        # Delete object
        test_obj.delete()
        self._wait_es()

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 1)

        # Create incorrect object (User object) and try to index it
        user_model = get_user_model()
        test_incorrect = user_model.objects.create(username='user_one')
        TestSearchIndex().build(test_incorrect)

    def test_management_commands(self):
        from .test_app.models import TestModel
        from .test_app.elastic_indexes import TestSearchDocument

        # Prepare test data
        TestModel.objects.create(name='Object name', number=43)
        self._wait_es()

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 1)

        # Purge index
        call_command('elastic_purge', interactive=False, verbosity=0)
        self._wait_es()

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 0)

        # Recreate index
        call_command('elastic_index', interactive=False, verbosity=0)
        self._wait_es()

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 1)

    def test_permissions(self):
        from .test_app.models import TestModel
        from .test_app.elastic_indexes import TestSearchDocument

        # Prepare users and groups
        user_model = get_user_model()
        user_1 = user_model.objects.create(username='user_one')
        user_2 = user_model.objects.create(username='user_two')
        user_3 = user_model.objects.create(username='user_three')
        group = Group.objects.create(name='group')

        # Create test object
        test_obj = TestModel.objects.create(name='Object name', number=43)
        assign_perm('view_model', user_1, test_obj)
        assign_perm('view_model', user_2, test_obj)
        assign_perm('view_model', group, test_obj)
        self._wait_es()

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(es_objects[0].users_with_permissions, [user_1.pk, user_2.pk])
        self.assertEqual(es_objects[0].groups_with_permissions, [group.pk])

        # Change permissions
        remove_perm('view_model', user_2, test_obj)
        assign_perm('view_model', user_3, test_obj)
        self._wait_es()

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(es_objects[0].users_with_permissions, [user_1.pk, user_3.pk])
        self.assertEqual(es_objects[0].groups_with_permissions, [group.pk])
