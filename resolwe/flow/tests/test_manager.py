# pylint: disable=missing-docstring
from __future__ import absolute_import, division, print_function, unicode_literals

import os

from django.db import transaction

from guardian.shortcuts import assign_perm

from resolwe.flow.managers import manager
from resolwe.flow.models import Collection, Data, DataDependency, DescriptorSchema, Process
from resolwe.test import ProcessTestCase

PROCESSES_DIR = os.path.join(os.path.dirname(__file__), 'processes')


class TestManager(ProcessTestCase):

    def setUp(self):
        super(TestManager, self).setUp()

        self.collection = Collection.objects.create(contributor=self.contributor)

        self._register_schemas(path=[PROCESSES_DIR])

    def test_create_data(self):
        """Test that manager is run when new object is created."""
        process = Process.objects.filter(slug='test-min').latest()
        data = Data.objects.create(
            name='Test data',
            contributor=self.contributor,
            process=process,
        )

        manager.execution_barrier()

        data.refresh_from_db()
        self.assertEqual(data.status, Data.STATUS_DONE)

    def test_spawned_process(self):
        """Test that manager is run for spawned processes and permissions are copied."""
        DescriptorSchema.objects.create(
            name='Test schema', slug='test-schema', contributor=self.contributor
        )
        spawned_process = Process.objects.filter(slug='test-save-file').latest()
        # Patch the process to create Entity, so its bahaviour can be tested.
        spawned_process.flow_collection = 'test-schema'
        spawned_process.save()

        process = Process.objects.filter(slug='test-spawn-new').latest()

        with transaction.atomic():
            data = Data.objects.create(
                name='Test data',
                contributor=self.contributor,
                process=process,
            )
            self.collection.data.add(data)
            assign_perm('view_data', self.user, data)

        manager.execution_barrier()

        # Created and spawned objects should be done.
        self.assertEqual(Data.objects.filter(status=Data.STATUS_DONE).count(), 2)

        # Check that permissions are inherited.
        child = Data.objects.last()
        self.assertTrue(self.user.has_perm('view_data', child))
        self.assertEqual(child.collection_set.count(), 1)
        self.assertEqual(child.collection_set.first().pk, self.collection.pk)
        self.assertEqual(child.entity_set.first().collections.count(), 1)
        self.assertEqual(child.entity_set.first().collections.first().pk, self.collection.pk)

    def test_workflow(self):
        """Test that manager is run for workflows."""
        workflow = Process.objects.filter(slug='test-workflow-1').latest()
        data1 = Data.objects.create(name='Test data 1', contributor=self.contributor, process=workflow,
                                    input={'param1': 'world'})
        data2 = Data.objects.create(name='Test data 2', contributor=self.contributor, process=workflow,
                                    input={'param1': 'foobar'})

        manager.execution_barrier()

        # Created and spawned objects should be done.
        self.assertEqual(Data.objects.filter(status=Data.STATUS_DONE).count(), 6)

        # Check correct dependency type is created.
        self.assertEqual({d.kind for d in data1.children_dependency.all()}, {DataDependency.KIND_SUBPROCESS})
        self.assertEqual({d.kind for d in data2.children_dependency.all()}, {DataDependency.KIND_SUBPROCESS})

    def test_dependencies(self):
        """Test that manager handles dependencies correctly."""
        process_parent = Process.objects.filter(slug='test-dependency-parent').latest()
        process_child = Process.objects.filter(slug='test-dependency-child').latest()
        data_parent = Data.objects.create(name='Test parent', contributor=self.contributor,
                                          process=process_parent)
        data_child1 = Data.objects.create(name='Test child', contributor=self.contributor,
                                          process=process_child, input={})
        data_child2 = Data.objects.create(name='Test child', contributor=self.contributor,
                                          process=process_child, input={'parent': data_parent.pk})
        data_child3 = Data.objects.create(name='Test child', contributor=self.contributor,
                                          process=process_child, input={'parent': None})

        manager.execution_barrier()

        data_parent.refresh_from_db()
        data_child1.refresh_from_db()
        data_child2.refresh_from_db()
        data_child3.refresh_from_db()
        self.assertEqual(data_parent.status, Data.STATUS_DONE)
        self.assertEqual(data_child1.status, Data.STATUS_DONE)
        self.assertEqual(data_child2.status, Data.STATUS_DONE)
        self.assertEqual(data_child3.status, Data.STATUS_DONE)
