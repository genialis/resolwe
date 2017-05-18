# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import io
import time

import six

from django.apps import apps
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser, Group
from django.core.management import call_command
from django.test import override_settings

from guardian.shortcuts import assign_perm, remove_perm

# Import signals manually because we ignore them in App ready for tests
from resolwe.elastic import signals  # pylint: disable=unused-import
from resolwe.elastic.builder import index_builder
from resolwe.test import ElasticSearchTestCase

CUSTOM_SETTINGS = {
    'INSTALLED_APPS': settings.INSTALLED_APPS + ('resolwe.elastic.tests.test_app',),
}


@override_settings(**CUSTOM_SETTINGS)
class IndexTest(ElasticSearchTestCase):

    def setUp(self):
        from .test_app.elastic_indexes import TestSearchIndex, TestAnalyzerSearchIndex

        super(IndexTest, self).setUp()

        apps.clear_cache()
        call_command('migrate', verbosity=0, interactive=False, load_initial_data=False)

        index_builder.indexes = [
            TestSearchIndex(),
            TestAnalyzerSearchIndex(),
        ]

        index_builder.register_signals()

    def tearDown(self):
        index_builder.destroy()
        super(IndexTest, self).tearDown()

    def test_mapping_multiple_times(self):
        index_builder.create_mappings()
        index_builder.create_mappings()

    def test_indexing(self):
        from .test_app.models import TestModel
        from .test_app.elastic_indexes import TestSearchDocument, TestSearchIndex

        # Create new object
        test_obj = TestModel.objects.create(name='Object name', number=43)

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 1)
        self.assertEqual(es_objects[0].name, 'Object name')
        self.assertEqual(es_objects[0].num, 43)
        self.assertEqual(es_objects[0].json['key'], 'value')

        # Update existing object
        test_obj.name = 'Another name'
        test_obj.save()

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 1)
        self.assertEqual(es_objects[0].name, 'Another name')
        self.assertEqual(es_objects[0].num, 43)
        self.assertEqual(es_objects[0].json['key'], 'value')

        # Create another object
        TestModel.objects.create(name='Another object', number=3)

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 2)

        # Delete object
        test_obj.delete()

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 1)

        # Create incorrect object (User object) and try to index it
        user_model = get_user_model()
        test_incorrect = user_model.objects.create(username='user_one')
        TestSearchIndex().build(test_incorrect)

    def test_management_commands(self):
        from .test_app.models import TestModel
        from .test_app.elastic_indexes import TestSearchDocument, TestAnalyzerSearchDocument

        # Prepare test data
        TestModel.objects.create(name='Object name', number=43)

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 1)

        # Purge index
        call_command('elastic_purge', interactive=False, verbosity=0)

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 0)

        # Recreate index
        call_command('elastic_index', interactive=False, verbosity=0)

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 1)

        # Purge index
        call_command('elastic_purge', interactive=False, verbosity=0)

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 0)

        # Recreate only a specific index
        call_command('elastic_index', index=['TestAnalyzerSearchIndex'], interactive=False, verbosity=0)

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 0)
        es_objects = TestAnalyzerSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 1)

        # Purge only a specific index
        call_command('elastic_purge', index=['TestAnalyzerSearchIndex'], interactive=False, verbosity=0)
        # Sleep is required as otherwise ES may reject queries during index re-create.
        time.sleep(1)

        es_objects = TestAnalyzerSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 0)

        call_command('elastic_index', exclude=['TestAnalyzerSearchIndex'], interactive=False, verbosity=0)
        es_objects = TestAnalyzerSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 0)

        call_command('elastic_index', interactive=False, verbosity=0)
        es_objects = TestAnalyzerSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 1)

        call_command('elastic_purge', exclude=['TestAnalyzerSearchIndex'], interactive=False, verbosity=0)
        # Sleep is required as otherwise ES may reject queries during index re-create.
        time.sleep(1)

        es_objects = TestAnalyzerSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 1)

        # Recreate an invalid index
        if six.PY2:
            output = io.BytesIO()
        else:
            output = io.StringIO()

        call_command('elastic_index', index=['InvalidIndex'], interactive=False, verbosity=0, stderr=output)
        self.assertIn("Unknown index: InvalidIndex", output.getvalue())

        call_command('elastic_index', exclude=['InvalidIndex'], interactive=False, verbosity=0, stderr=output)
        self.assertIn("Unknown index: InvalidIndex", output.getvalue())

        # Purge an invalid index
        call_command('elastic_purge', index=['InvalidIndex'], interactive=False, verbosity=0, stderr=output)
        self.assertIn("Unknown index: InvalidIndex", output.getvalue())

        call_command('elastic_purge', exclude=['InvalidIndex'], interactive=False, verbosity=0, stderr=output)
        self.assertIn("Unknown index: InvalidIndex", output.getvalue())

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
        assign_perm('view_testmodel', user_1, test_obj)
        assign_perm('view_testmodel', user_2, test_obj)
        assign_perm('view_testmodel', group, test_obj)

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(es_objects[0].users_with_permissions, [user_1.pk, user_2.pk])
        self.assertEqual(es_objects[0].groups_with_permissions, [group.pk])
        self.assertEqual(es_objects[0].public_permission, False)

        # Change permissions
        remove_perm('view_testmodel', user_2, test_obj)
        assign_perm('view_testmodel', user_3, test_obj)

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(es_objects[0].users_with_permissions, [user_1.pk, user_3.pk])
        self.assertEqual(es_objects[0].groups_with_permissions, [group.pk])
        self.assertEqual(es_objects[0].public_permission, False)

        # Change permissions
        assign_perm('view_testmodel', AnonymousUser(), test_obj)

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(es_objects[0].public_permission, True)

    def test_field_name(self):
        from .test_app.models import TestModel
        from .test_app.elastic_indexes import TestSearchDocument

        TestModel.objects.create(name='Hello world FOO_BAR-G17-SA', number=42)

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 1)

        es_objects = TestSearchDocument.search().query('match', field_name='hello').execute()
        self.assertEqual(len(es_objects), 1)

        es_objects = TestSearchDocument.search().query('match', field_name='world').execute()
        self.assertEqual(len(es_objects), 1)

        es_objects = TestSearchDocument.search().query('match', **{'field_name.raw': 'hello'}).execute()
        self.assertEqual(len(es_objects), 0)

        es_objects = TestSearchDocument.search().query(
            'match', **{'field_name.raw': 'Hello world FOO_BAR-G17-SA'}
        ).execute()
        self.assertEqual(len(es_objects), 1)

        es_objects = TestSearchDocument.search().query('match', field_name='foo').execute()
        self.assertEqual(len(es_objects), 1)

        es_objects = TestSearchDocument.search().query('match', field_name='bar').execute()
        self.assertEqual(len(es_objects), 1)

        es_objects = TestSearchDocument.search().query('match', field_name='g17').execute()
        self.assertEqual(len(es_objects), 1)

        es_objects = TestSearchDocument.search().query('match', field_name='g17-sa').execute()
        self.assertEqual(len(es_objects), 1)

        es_objects = TestSearchDocument.search().query('match', field_name='17').execute()
        self.assertEqual(len(es_objects), 1)

    def test_field_process_type(self):
        from .test_app.models import TestModel
        from .test_app.elastic_indexes import TestSearchDocument

        TestModel.objects.create(field_process_type='data:geneset', number=42)
        TestModel.objects.create(field_process_type='data:geneset:venn', number=42)
        TestModel.objects.create(field_process_type='data:geneset:venn:omg', number=42)

        es_objects = TestSearchDocument.search().execute()
        self.assertEqual(len(es_objects), 3)

        es_objects = TestSearchDocument.search().query('match', field_process_type='data').execute()
        self.assertEqual(len(es_objects), 3)

        es_objects = TestSearchDocument.search().query('match', field_process_type='data:geneset').execute()
        self.assertEqual(len(es_objects), 3)

        es_objects = TestSearchDocument.search().query('match', field_process_type='data:geneset:venn').execute()
        self.assertEqual(len(es_objects), 2)

        es_objects = TestSearchDocument.search().query('match', field_process_type='data:geneset:venn:omg').execute()
        self.assertEqual(len(es_objects), 1)

        # Check if tokenizer did not include intermediate terms.
        es_objects = TestSearchDocument.search().query('match', field_process_type='geneset').execute()
        self.assertEqual(len(es_objects), 0)
        es_objects = TestSearchDocument.search().query('match', field_process_type='venn').execute()
        self.assertEqual(len(es_objects), 0)
        es_objects = TestSearchDocument.search().query('match', field_process_type='omg').execute()
        self.assertEqual(len(es_objects), 0)

    def test_dependencies(self):
        from .test_app.models import TestModelWithDependency, TestDependency
        from .test_app.elastic_indexes import TestModelWithDependencyDocument, TestModelWithFilterDependencyDocument

        model = TestModelWithDependency.objects.create(name='Deps')
        dep1 = TestDependency.objects.create(name='one')
        dep2 = TestDependency.objects.create(name='two')
        dep3 = TestDependency.objects.create(name='three')
        model.dependencies.add(dep1)
        model.dependencies.add(dep2)
        dep3.testmodelwithdependency_set.add(model)

        es_objects = TestModelWithDependencyDocument.search().query('match', name='deps').execute()
        self.assertEqual(len(es_objects), 1)

        es_objects = TestModelWithDependencyDocument.search().query('match', name='one').execute()
        self.assertEqual(len(es_objects), 1)

        es_objects = TestModelWithDependencyDocument.search().query('match', name='two').execute()
        self.assertEqual(len(es_objects), 1)

        es_objects = TestModelWithDependencyDocument.search().query('match', name='three').execute()
        self.assertEqual(len(es_objects), 1)

        es_objects = TestModelWithDependencyDocument.search().query('match', name='four').execute()
        self.assertEqual(len(es_objects), 0)

        dep3.name = 'four'
        dep3.save()

        es_objects = TestModelWithDependencyDocument.search().query('match', name='four').execute()
        self.assertEqual(len(es_objects), 1)

        es_objects = TestModelWithDependencyDocument.search().query('match', name='three').execute()
        self.assertEqual(len(es_objects), 0)

        # Ensure that previous updates did not cause the filtered version to be updated.
        es_objects = TestModelWithFilterDependencyDocument.search().execute()
        self.assertEqual(len(es_objects), 1)
        # If the filtered version would be updated, this would instead equal 'Deps: one, two, four'.
        self.assertEqual(es_objects[0].name, 'Deps: ')

        dep4 = TestDependency.objects.create(name='hello')
        dep5 = TestDependency.objects.create(name='hello')
        model.dependencies.add(dep4)
        dep5.testmodelwithdependency_set.add(model)

        es_objects = TestModelWithFilterDependencyDocument.search().execute()
        self.assertEqual(len(es_objects), 1)
        # It is correct that even non-dependencies are contained in the name as dependencies are
        # only used to determine when to trigger updates.
        self.assertEqual(es_objects[0].name, 'Deps: one, two, four, hello, hello')
