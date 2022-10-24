# pylint: disable=missing-docstring
import os

from asgiref.sync import async_to_sync

from resolwe.flow.managers import manager
from resolwe.flow.managers.utils import disable_auto_calls
from resolwe.flow.models import (
    Collection,
    Data,
    DataDependency,
    DescriptorSchema,
    Process,
)
from resolwe.permissions.models import Permission
from resolwe.test import ProcessTestCase, TransactionTestCase

PROCESSES_DIR = os.path.join(os.path.dirname(__file__), "processes")


class TestManager(ProcessTestCase):
    def setUp(self):
        super().setUp()

        self.collection = Collection.objects.create(contributor=self.contributor)

        self._register_schemas(processes_paths=[PROCESSES_DIR])

    def test_create_data(self):
        """Test that manager is run when new object is created."""
        process = Process.objects.filter(slug="test-min").latest()
        data = Data.objects.create(
            name="Test data",
            contributor=self.contributor,
            process=process,
        )

        data.refresh_from_db()
        self.assertEqual(data.status, Data.STATUS_DONE)

    def test_spawned_process(self):
        """Test that manager is run for spawned processes and permissions are copied."""
        DescriptorSchema.objects.create(
            name="Test schema", slug="test-schema", contributor=self.contributor
        )
        spawned_process = Process.objects.filter(slug="test-save-file").latest()
        # Patch the process to create Entity, so its bahaviour can be tested.
        spawned_process.entity_type = "test-schema"
        spawned_process.entity_descriptor_schema = "test-schema"
        spawned_process.save()

        # Make sure user can spawn the process.
        spawned_process.set_permission(Permission.VIEW, self.contributor)
        self.collection.set_permission(Permission.VIEW, self.user)
        Data.objects.create(
            name="Test data",
            contributor=self.contributor,
            process=Process.objects.filter(slug="test-spawn-new").latest(),
            collection=self.collection,
        )

        # Created and spawned objects should be done.
        self.assertEqual(Data.objects.filter(status=Data.STATUS_DONE).count(), 2)

        # Check that permissions are inherited.
        child = Data.objects.last()
        self.assertTrue(self.user.has_perm(Permission.VIEW, child))
        self.assertEqual(child.collection.pk, self.collection.pk)
        self.assertEqual(child.entity.collection.pk, self.collection.pk)

    def test_workflow(self):
        """Test that manager is run for workflows."""
        workflow = Process.objects.filter(slug="test-workflow-1").latest()
        data1 = Data.objects.create(
            name="Test data 1",
            contributor=self.contributor,
            process=workflow,
            input={"param1": "world"},
        )
        data2 = Data.objects.create(
            name="Test data 2",
            contributor=self.contributor,
            process=workflow,
            input={"param1": "foobar"},
        )

        # Created and spawned objects should be done.
        self.assertEqual(Data.objects.filter(status=Data.STATUS_DONE).count(), 6)

        # Check correct dependency type is created.
        self.assertEqual(
            {d.kind for d in data1.children_dependency.all()},
            {DataDependency.KIND_SUBPROCESS},
        )
        self.assertEqual(
            {d.kind for d in data2.children_dependency.all()},
            {DataDependency.KIND_SUBPROCESS},
        )

    def test_dependencies(self):
        """Test that manager handles dependencies correctly."""
        process_parent = Process.objects.filter(slug="test-dependency-parent").latest()
        process_child = Process.objects.filter(slug="test-dependency-child").latest()
        data_parent = Data.objects.create(
            name="Test parent", contributor=self.contributor, process=process_parent
        )
        data_child1 = Data.objects.create(
            name="Test child",
            contributor=self.contributor,
            process=process_child,
            input={},
        )
        data_child2 = Data.objects.create(
            name="Test child",
            contributor=self.contributor,
            process=process_child,
            input={"parent": data_parent.pk},
        )
        data_child3 = Data.objects.create(
            name="Test child",
            contributor=self.contributor,
            process=process_child,
            input={"parent": None},
        )

        data_parent.refresh_from_db()
        data_child1.refresh_from_db()
        data_child2.refresh_from_db()
        data_child3.refresh_from_db()

        # Check locks are created in manager.
        self.assertFalse(data_parent.access_logs.exists())
        self.assertFalse(data_child1.access_logs.exists())
        self.assertTrue(data_child2.access_logs.exists())
        self.assertFalse(data_child3.access_logs.exists())

        # Check that the data_parent location was locked.
        access_log = data_child2.access_logs.get()
        self.assertEqual(
            access_log.storage_location.file_storage.data.get().id, data_parent.id
        )
        # Check that the log is released.
        self.assertIsNotNone(access_log.started)
        self.assertIsNotNone(access_log.finished)

        # Check status.
        self.assertEqual(data_parent.status, Data.STATUS_DONE)
        self.assertEqual(data_child1.status, Data.STATUS_DONE)
        self.assertEqual(data_child2.status, Data.STATUS_DONE)
        self.assertEqual(data_child3.status, Data.STATUS_DONE)

    def test_process_notifications(self):
        process = Process.objects.filter(slug="test-process-notifications").latest()
        data = Data.objects.create(
            name="Test data",
            contributor=self.contributor,
            process=process,
        )

        data.refresh_from_db()

        self.assertEqual(len(data.process_info), 2)
        self.assertEqual(data.process_info[0], "abc")
        self.assertEqual(data.process_info[1][-5:], "xx...")

        self.assertEqual(len(data.process_warning), 1)
        self.assertEqual(data.process_warning[0][-5:], "yy...")

        self.assertEqual(len(data.process_error), 1)
        self.assertEqual(data.process_error[0][-5:], "zz...")


class TransactionTestManager(TransactionTestCase):
    @disable_auto_calls()
    def test_communicate(self):
        process = Process.objects.create(
            name="Input process",
            contributor=self.contributor,
            type="data:test:",
            input_schema=[
                {
                    "name": "input_data",
                    "type": "data:test:",
                    "required": False,
                },
            ],
        )

        data_1 = Data.objects.create(contributor=self.contributor, process=process)
        data_2 = Data.objects.create(
            contributor=self.contributor,
            process=process,
            input={"input_data": data_1.id},
        )
        Data.objects.create(contributor=self.contributor, process=process)
        Data.objects.create(contributor=self.contributor, process=process)

        self.assertEqual(Data.objects.filter(status=Data.STATUS_RESOLVING).count(), 4)

        # Allow unfinished data objects to exist when checking for execution
        # barrier condition in the dispatcher.
        async_to_sync(manager.communicate)(data_id=data_1.pk, run_sync=True)

        data_1.refresh_from_db()
        self.assertEqual(data_1.status, Data.STATUS_WAITING)
        self.assertEqual(Data.objects.filter(status=Data.STATUS_RESOLVING).count(), 3)

        data_1.status = Data.STATUS_DONE
        data_1.save()

        # Process object's children.
        async_to_sync(manager.communicate)(data_id=data_1.pk, run_sync=True)

        data_2.refresh_from_db()
        self.assertEqual(data_2.status, Data.STATUS_WAITING)
        self.assertEqual(Data.objects.filter(status=Data.STATUS_RESOLVING).count(), 2)

        # Process all objects.
        async_to_sync(manager.communicate)(run_sync=True)

        self.assertEqual(Data.objects.filter(status=Data.STATUS_RESOLVING).count(), 0)
