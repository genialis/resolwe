# pylint: disable=missing-docstring
import asyncio
import os
import threading
from copy import deepcopy
from datetime import timedelta
from time import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from asgiref.sync import async_to_sync
from django.contrib.auth import get_user_model
from django.db import connection, transaction
from django.db.backends.signals import connection_created
from django.db.utils import OperationalError
from django.test import SimpleTestCase, override_settings
from django.utils.timezone import now

from resolwe.flow.executors.socket_utils import Message
from resolwe.flow.managers import manager
from resolwe.flow.managers.dispatcher import DEFAULT_CONNECTOR
from resolwe.flow.managers.listener import ExecutorListener
from resolwe.flow.managers.listener.authenticator import ZMQAuthenticator
from resolwe.flow.managers.listener.basic_commands_plugin import BasicCommands
from resolwe.flow.managers.listener.database import (
    DEFAULT_DATABASE_WRITE_ATTEMPTS,
    is_retriable_database_error,
    retry_database_writes,
    write_transaction,
)
from resolwe.flow.managers.listener.listener import (
    DATABASE_TIMEOUTS_DISPATCH_UID,
    STALLED_DATA_WARNING,
    Processor,
    enable_database_timeouts,
)
from resolwe.flow.managers.listener.permission_plugin import permission_manager
from resolwe.flow.managers.listener.python_process_plugin import PythonProcess
from resolwe.flow.managers.protocol import ExecutorProtocol, WorkerProtocol
from resolwe.flow.managers.utils import disable_auto_calls
from resolwe.flow.models import (
    Collection,
    Data,
    DataDependency,
    DescriptorSchema,
    Process,
    Storage,
    Worker,
)
from resolwe.permissions.models import Permission
from resolwe.storage.models import AccessLog, FileStorage, StorageLocation
from resolwe.test import ProcessTestCase, TransactionTestCase

PROCESSES_DIR = os.path.join(os.path.dirname(__file__), "processes")


class TestManager(ProcessTestCase):
    def setUp(self):
        super().setUp()

        self.collection = Collection.objects.create(contributor=self.contributor)
        self._register_schemas(processes_paths=[PROCESSES_DIR])
        manager._processes_ignore = None
        manager._processes_allow = None

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

    def test_ignore_allow_list(self):
        process = Process.objects.filter(slug="test-min").latest()

        # Ignored processes should not trigger processing.
        manager._processes_ignore = ["test-min"]
        data = Data.objects.create(
            name="Test data",
            contributor=self.contributor,
            process=process,
        )
        data.refresh_from_db()
        self.assertEqual(data.status, Data.STATUS_RESOLVING)

        # Ignore should have precedence.
        manager._processes_ignore = ["test-min"]
        manager._processes_allow = ["test-min"]
        data = Data.objects.create(
            name="Test data",
            contributor=self.contributor,
            process=process,
        )
        data.refresh_from_db()
        self.assertEqual(data.status, Data.STATUS_RESOLVING)

        # Allowing some processes shoud disable others.
        manager._processes_ignore = None
        manager._processes_allow = ["test-something-else"]
        data = Data.objects.create(
            name="Test data",
            contributor=self.contributor,
            process=process,
        )
        data.refresh_from_db()
        self.assertEqual(data.status, Data.STATUS_RESOLVING)

    def test_spawned_process(self):
        """Test that manager is run for spawned processes and permissions are copied."""
        DescriptorSchema.objects.create(
            name="Test schema", slug="test-schema", contributor=self.contributor
        )
        spawned_process = Process.objects.filter(slug="test-save-file").latest()
        # Patch the process to create Entity, so its bahaviour can be tested.
        spawned_process.entity_type = "test-schema"
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


class ZMQAuthenticatorTest(SimpleTestCase):
    """Test the authorization pruning of the listener authenticator."""

    def test_prune_authorizations(self):
        """Only workers without active data objects are pruned."""
        authenticator = ZMQAuthenticator()
        authenticator.authorize_client(b"active", 1)
        authenticator.authorize_client(b"stale", 2)
        authenticator.authorize_client(b"mixed", 2)
        authenticator.authorize_client(b"mixed", 3)

        authenticator.prune_authorizations({1, 3})

        self.assertTrue(authenticator.can_access_data(b"active", 1))
        self.assertFalse(authenticator.can_access_data(b"stale", 2))
        # A worker with at least one active data object keeps all its
        # authorizations.
        self.assertTrue(authenticator.can_access_data(b"mixed", 2))
        self.assertTrue(authenticator.can_access_data(b"mixed", 3))

    def test_prune_authorizations_candidates(self):
        """Entries added after the candidate snapshot survive the pruning."""
        authenticator = ZMQAuthenticator()
        authenticator.authorize_client(b"active", 1)
        authenticator.authorize_client(b"stale", 2)

        candidate_keys = authenticator.authorization_keys()
        # A worker that connected after the caller snapshotted the keys: its
        # data id is not in the (stale) active set, but the entry must
        # survive until the next pruning cycle.
        authenticator.authorize_client(b"new", 99)

        authenticator.prune_authorizations({1}, candidate_keys)

        self.assertTrue(authenticator.can_access_data(b"active", 1))
        self.assertFalse(authenticator.can_access_data(b"stale", 2))
        self.assertTrue(authenticator.can_access_data(b"new", 99))


class StalledDataRequeueTest(TransactionTestCase):
    """Test requeueing of data objects whose dispatch was interrupted."""

    def setUp(self):
        super().setUp()
        self.process = Process.objects.create(
            name="Test process",
            contributor=self.contributor,
            type="data:test:",
            run={"language": "bash", "program": "true"},
        )
        self.processor = Processor(None)

    def _create_data(self, **updates):
        """Create a data object and modify it, bypassing auto_now fields."""
        with disable_auto_calls():
            data = Data.objects.create(
                name="Test data", contributor=self.contributor, process=self.process
            )
            Worker.objects.create(
                data=data,
                status=Worker.STATUS_PREPARING,
                public_key=b"",
                private_key=b"",
            )
        if updates:
            Data.objects.filter(pk=data.pk).update(**updates)
            data.refresh_from_db()
        return data

    def _process_stalled_data(self):
        with disable_auto_calls():
            return self.processor._process_stalled_data()

    def test_requeue_stalled_data(self):
        """Stalled data object is returned to the resolving status."""
        data = self._create_data(
            status=Data.STATUS_WAITING,
            scheduled=None,
            modified=now() - timedelta(hours=1),
        )
        # Simulate a worker left in a final status by a failed dispatch.
        Worker.objects.filter(data=data).update(status=Worker.STATUS_ERROR_PREPARING)

        requeued, failed = self._process_stalled_data()

        data.refresh_from_db()
        self.assertEqual(requeued, [data.pk])
        self.assertEqual(failed, [])
        self.assertEqual(data.status, Data.STATUS_RESOLVING)
        self.assertEqual(len(data.process_warning), 1)
        self.assertTrue(data.process_warning[0].startswith(STALLED_DATA_WARNING))
        self.assertEqual(data.worker.status, Worker.STATUS_PREPARING)

    def test_active_data_not_requeued(self):
        """Objects that are being dispatched or submitted are left alone."""
        # Freshly claimed object (dispatch still in progress).
        fresh = self._create_data(status=Data.STATUS_WAITING, scheduled=None)
        # Object already submitted to the workload connector.
        submitted = self._create_data(
            status=Data.STATUS_WAITING,
            scheduled=now() - timedelta(hours=1),
            modified=now() - timedelta(hours=1),
        )
        # Object waiting for its dependencies.
        resolving = self._create_data(modified=now() - timedelta(hours=1))

        requeued, failed = self._process_stalled_data()

        self.assertEqual(requeued, [])
        self.assertEqual(failed, [])
        for data, status in [
            (fresh, Data.STATUS_WAITING),
            (submitted, Data.STATUS_WAITING),
            (resolving, Data.STATUS_RESOLVING),
        ]:
            data.refresh_from_db()
            self.assertEqual(data.status, status)
            self.assertEqual(data.process_warning, [])

    def test_requeue_nudges_dispatcher(self):
        """The dispatcher is nudged for every requeued data object."""
        data = self._create_data(
            status=Data.STATUS_WAITING,
            scheduled=None,
            modified=now() - timedelta(hours=1),
        )

        with (
            patch(
                "resolwe.flow.managers.listener.listener.consumer.send_event"
            ) as send_event,
            disable_auto_calls(),
        ):
            async_to_sync(self.processor.requeue_stalled_data)()

        data.refresh_from_db()
        self.assertEqual(data.status, Data.STATUS_RESOLVING)
        send_event.assert_called_once_with(
            {
                WorkerProtocol.COMMAND: WorkerProtocol.COMMUNICATE,
                WorkerProtocol.COMMUNICATE_EXTRA: {"data_id": data.pk},
            }
        )

    def test_non_executor_data_not_requeued(self):
        """Objects whose process never runs in the executor are left alone.

        Processes without a run section and workflows legitimately sit in the
        waiting status without the scheduled timestamp; they must not be
        treated as interrupted dispatches.
        """
        for run in [{}, {"language": "workflow"}]:
            with self.subTest(run=run):
                self.process = Process.objects.create(
                    name="Non-executor process",
                    contributor=self.contributor,
                    type="data:test:",
                    run=run,
                )
                data = self._create_data(
                    status=Data.STATUS_WAITING,
                    scheduled=None,
                    modified=now() - timedelta(hours=1),
                )

                requeued, failed = self._process_stalled_data()

                data.refresh_from_db()
                self.assertEqual((requeued, failed), ([], []))
                self.assertEqual(data.status, Data.STATUS_WAITING)
                self.assertEqual(data.process_warning, [])

    def test_stalled_data_fails_after_max_requeues(self):
        """Object is marked failed when requeueing does not help."""
        data = self._create_data(
            status=Data.STATUS_WAITING,
            scheduled=None,
            modified=now() - timedelta(hours=1),
            process_warning=[STALLED_DATA_WARNING] * 3,
        )

        requeued, failed = self._process_stalled_data()

        data.refresh_from_db()
        self.assertEqual(requeued, [])
        self.assertEqual(failed, [data.pk])
        self.assertEqual(data.status, Data.STATUS_ERROR)
        self.assertEqual(len(data.process_error), 1)
        self.assertEqual(data.worker.status, Worker.STATUS_ERROR_PREPARING)

    def test_requeue_vanished_submission(self):
        """Submitted object whose task has vanished is requeued."""
        data = self._create_data(
            status=Data.STATUS_WAITING,
            scheduled=now() - timedelta(hours=1),
            modified=now() - timedelta(hours=1),
        )
        connector = manager.connectors[DEFAULT_CONNECTOR]

        # The connector cannot determine the task state: leave the object be.
        # This is also what the default base connector implementation returns.
        with patch.object(connector, "is_active", return_value=None):
            requeued, failed = self._process_stalled_data()
        data.refresh_from_db()
        self.assertEqual((requeued, failed), ([], []))
        self.assertEqual(data.status, Data.STATUS_WAITING)

        # The task is still queued or running: leave the object be.
        with patch.object(connector, "is_active", return_value=True):
            requeued, failed = self._process_stalled_data()
        data.refresh_from_db()
        self.assertEqual((requeued, failed), ([], []))
        self.assertEqual(data.status, Data.STATUS_WAITING)

        # The task is gone: requeue the object.
        with patch.object(connector, "is_active", return_value=False):
            requeued, failed = self._process_stalled_data()
        data.refresh_from_db()
        self.assertEqual((requeued, failed), ([data.pk], []))
        self.assertEqual(data.status, Data.STATUS_RESOLVING)
        self.assertIsNone(data.scheduled)
        self.assertEqual(len(data.process_warning), 1)
        self.assertTrue(data.process_warning[0].startswith(STALLED_DATA_WARNING))

        # A recently submitted object is not even checked with the connector.
        recent = self._create_data(status=Data.STATUS_WAITING, scheduled=now())
        with patch.object(connector, "is_active", return_value=False) as is_active:
            requeued, failed = self._process_stalled_data()
        recent.refresh_from_db()
        self.assertEqual(recent.status, Data.STATUS_WAITING)
        self.assertNotIn(recent.pk, requeued)
        self.assertNotIn(
            recent.pk, [call.args[0].pk for call in is_active.call_args_list]
        )

    def test_kubernetes_is_active(self):
        """The kubernetes connector reports the state of its jobs."""
        from resolwe.flow.managers.workload_connectors import (
            kubernetes as kubernetes_connector,
        )

        data = self._create_data()
        connector = kubernetes_connector.Connector()

        with (
            patch.object(connector, "_load_kubernetes_config"),
            patch.object(
                kubernetes_connector.kubernetes.client, "BatchV1Api"
            ) as batch_api,
        ):
            list_jobs = batch_api.return_value.list_namespaced_job

            # No job exists for the data object.
            list_jobs.return_value = SimpleNamespace(items=[])
            self.assertIs(connector.is_active(data), False)

            # A job without a terminal condition is active, even when its pod
            # is still pending in the cluster queue.
            pending = SimpleNamespace(status=SimpleNamespace(conditions=None))
            list_jobs.return_value = SimpleNamespace(items=[pending])
            self.assertIs(connector.is_active(data), True)

            # A permanently failed job can never run again.
            failed_condition = SimpleNamespace(type="Failed", status="True")
            failed = SimpleNamespace(
                status=SimpleNamespace(conditions=[failed_condition])
            )
            list_jobs.return_value = SimpleNamespace(items=[failed])
            self.assertIs(connector.is_active(data), False)

            # A failed job of a previous run next to an active one.
            list_jobs.return_value = SimpleNamespace(items=[failed, pending])
            self.assertIs(connector.is_active(data), True)

            # The state cannot be determined on API errors.
            list_jobs.side_effect = Exception("API error")
            self.assertIsNone(connector.is_active(data))

    def test_kubernetes_is_active_bulk(self):
        """The kubernetes connector answers for all candidates at once."""
        from resolwe.flow.managers.workload_connectors import (
            kubernetes as kubernetes_connector,
        )

        data_active = self._create_data()
        data_finished = self._create_data()
        data_no_job = self._create_data()
        connector = kubernetes_connector.Connector()

        def job(data_id, conditions):
            return SimpleNamespace(
                metadata=SimpleNamespace(labels={"data_id": str(data_id)}),
                status=SimpleNamespace(conditions=conditions),
            )

        with (
            patch.object(connector, "_load_kubernetes_config"),
            patch.object(
                kubernetes_connector.kubernetes.client, "BatchV1Api"
            ) as batch_api,
        ):
            list_jobs = batch_api.return_value.list_namespaced_job
            list_jobs.return_value = SimpleNamespace(
                items=[
                    job(data_active.pk, None),
                    job(
                        data_finished.pk,
                        [SimpleNamespace(type="Complete", status="True")],
                    ),
                ]
            )

            self.assertEqual(
                connector.is_active_bulk([data_active, data_finished, data_no_job]),
                {
                    data_active.pk: True,
                    data_finished.pk: False,
                    data_no_job.pk: False,
                },
            )
            # All the candidates must be answered with a single API call.
            list_jobs.assert_called_once()

            # The state cannot be determined on API errors.
            list_jobs.side_effect = Exception("API error")
            self.assertEqual(
                connector.is_active_bulk([data_active]), {data_active.pk: None}
            )

            # Jobs without the data_id label mean the labeling assumption is
            # broken: the state is undetermined instead of everything being
            # reported inactive (and requeued).
            list_jobs.side_effect = None
            unlabeled = SimpleNamespace(
                metadata=SimpleNamespace(labels={"application": "resolwe"}),
                status=SimpleNamespace(conditions=None),
            )
            list_jobs.return_value = SimpleNamespace(items=[unlabeled])
            self.assertEqual(
                connector.is_active_bulk([data_active, data_no_job]),
                {data_active.pk: None, data_no_job.pk: None},
            )

    def _create_locked_input(self, data):
        """Create an input for the data object.

        The parent data object gets a storage location and is connected to
        the given data object as an input; the returned storage location is
        the one the dispatcher locks when the processing starts.
        """
        parent = self._create_data(status=Data.STATUS_DONE)
        file_storage = FileStorage.objects.create()
        location = StorageLocation.objects.create(
            file_storage=file_storage,
            url=str(file_storage.pk),
            status=StorageLocation.STATUS_DONE,
            connector_name="local",
        )
        file_storage.data.add(parent)
        DataDependency.objects.create(
            parent=parent, child=data, kind=DataDependency.KIND_IO
        )
        return location

    def test_failed_preparation_releases_input_locks(self):
        """A terminal preparation failure releases the input storage locks."""
        data = self._create_data(status=Data.STATUS_WAITING)
        self._create_locked_input(data)

        with (
            patch.object(manager, "_prepare_data_dir"),
            patch.object(
                manager.executor,
                "prepare_for_execution",
                side_effect=OSError("no space left on device"),
            ),
            disable_auto_calls(),
        ):
            manager._data_execute(data)

        data.refresh_from_db()
        self.assertEqual(data.status, Data.STATUS_ERROR)
        self.assertEqual(data.worker.status, Worker.STATUS_ERROR_PREPARING)
        access_log = AccessLog.objects.get(cause=data)
        self.assertIsNotNone(access_log.finished)

    def test_failed_submission_releases_input_locks(self):
        """A failed kubernetes submission releases the input storage locks."""
        from resolwe.flow.managers.workload_connectors import (
            kubernetes as kubernetes_connector,
        )

        data = self._create_data(status=Data.STATUS_WAITING)
        location = self._create_locked_input(data)
        access_log = AccessLog.objects.create(
            storage_location=location,
            reason="Input for data with id {}".format(data.pk),
            cause=data,
        )
        connector = kubernetes_connector.Connector()

        with (
            patch.object(connector, "_initialize_variables"),
            patch.object(connector, "start", side_effect=Exception("API error")),
            disable_auto_calls(),
        ):
            connector.submit(data, ["executor command host port protocol"])

        data.refresh_from_db()
        access_log.refresh_from_db()
        self.assertEqual(data.status, Data.STATUS_ERROR)
        self.assertEqual(data.worker.status, Worker.STATUS_ERROR_PREPARING)
        self.assertIsNotNone(access_log.finished)

    def test_run_claims_submission(self):
        """The manager submits a data object exactly once."""
        data = self._create_data(status=Data.STATUS_WAITING, scheduled=None)
        connector_mock = MagicMock()

        with patch.dict(manager.connectors, {DEFAULT_CONNECTOR: connector_mock}):
            manager.run(data, ["/bin/sh", "-c", "executor command"])
        data.refresh_from_db()
        self.assertIsNotNone(data.scheduled)
        connector_mock.submit.assert_called_once()

        # The second submission of the same object must be skipped.
        connector_mock.reset_mock()
        with patch.dict(manager.connectors, {DEFAULT_CONNECTOR: connector_mock}):
            manager.run(data, ["/bin/sh", "-c", "executor command"])
        connector_mock.submit.assert_not_called()

        # Objects requeued to another manager must not be submitted.
        requeued = self._create_data(status=Data.STATUS_RESOLVING, scheduled=None)
        with patch.dict(manager.connectors, {DEFAULT_CONNECTOR: connector_mock}):
            manager.run(requeued, ["/bin/sh", "-c", "executor command"])
        connector_mock.submit.assert_not_called()


class ListenerDatabaseTimeoutTest(TransactionTestCase):
    """Test the database timeouts applied to the listener connections."""

    DEFAULT_TIMEOUTS = {"lock_timeout": "30000", "statement_timeout": "600000"}

    def setUp(self):
        super().setUp()
        self.addCleanup(
            connection_created.disconnect, dispatch_uid=DATABASE_TIMEOUTS_DISPATCH_UID
        )
        # Drop the connection of this thread so the following tests start with
        # a connection without the timeouts.
        self.addCleanup(connection.close)

    def _current_timeouts(self) -> dict:
        """Return the timeouts (in milliseconds) of the current connection."""
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT name, setting FROM pg_settings "
                "WHERE name IN ('lock_timeout', 'statement_timeout')"
            )
            return dict(cursor.fetchall())

    def test_run_enables_timeouts(self):
        """The timeouts are enabled when the listener starts serving."""
        listener = ExecutorListener()
        protocol = MagicMock(communicate=AsyncMock())
        with (
            patch(
                "resolwe.flow.managers.listener.listener.enable_database_timeouts"
            ) as enable,
            patch.object(
                ExecutorListener,
                "listener_protocol",
                new_callable=PropertyMock,
                return_value=protocol,
            ),
        ):

            async def run():
                listener.should_stop.set()
                await listener.run()

            asyncio.run(run())
        enable.assert_called_once_with()
        protocol.stop_communicate.assert_called_once_with()

    def test_timeouts_on_new_connection(self):
        """Connections opened after the listener is created get the timeouts."""
        enable_database_timeouts()
        connection.close()
        self.assertEqual(self._current_timeouts(), self.DEFAULT_TIMEOUTS)

    def test_timeouts_on_open_connection(self):
        """Connections open when the listener is created get the timeouts."""
        connection.ensure_connection()
        enable_database_timeouts()
        self.assertEqual(self._current_timeouts(), self.DEFAULT_TIMEOUTS)

    @override_settings(
        LISTENER_DATABASE_LOCK_TIMEOUT=5, LISTENER_DATABASE_STATEMENT_TIMEOUT=None
    )
    def test_timeouts_from_settings(self):
        """The timeouts are read from the settings, None disables a timeout."""
        enable_database_timeouts()
        connection.close()
        self.assertEqual(
            self._current_timeouts(), {"lock_timeout": "5000", "statement_timeout": "0"}
        )

    def test_write_timeout_is_local(self):
        """The write timeout applies inside the write transaction only."""
        enable_database_timeouts()
        connection.close()
        with write_transaction():
            self.assertEqual(self._current_timeouts()["statement_timeout"], "30000")
        self.assertEqual(self._current_timeouts()["statement_timeout"], "600000")

    def test_write_transaction_rejects_nesting(self):
        """The write transaction refuses to run inside another transaction."""
        with transaction.atomic():
            with self.assertRaisesMessage(RuntimeError, "outermost"):
                with write_transaction():
                    pass  # pragma: no cover
            # The outer transaction is still usable.
            Storage.objects.exists()

    @override_settings(LISTENER_DATABASE_WRITE_TIMEOUT=1)
    def test_write_timeout_cancels_statement(self):
        """A write running longer than the write timeout is aborted."""
        with self.assertRaisesMessage(OperationalError, "statement timeout"):
            with write_transaction(), connection.cursor() as cursor:
                cursor.execute("SELECT pg_sleep(2)")

    @override_settings(LISTENER_DATABASE_LOCK_TIMEOUT=1)
    def test_blocked_statement_fails(self):
        """A statement waiting for a lock fails instead of waiting indefinitely.

        The scenario mirrors a listener handler creating a Storage object while
        another transaction holds an exclusive lock on the contributor row: the
        foreign key check of the insert has to wait for that transaction.
        """
        enable_database_timeouts()
        connection.close()

        blocker = connection.copy()
        blocker.set_autocommit(False)
        user_table = connection.ops.quote_name(get_user_model()._meta.db_table)
        with blocker.cursor() as cursor:
            cursor.execute(
                f"SELECT id FROM {user_table} WHERE id = %s FOR UPDATE",
                [self.contributor.pk],
            )
        # Release the lock eventually, so a missing timeout fails the test
        # instead of hanging it.
        release = threading.Timer(30, blocker.rollback)
        release.start()
        try:
            start = time()
            with self.assertRaisesMessage(OperationalError, "lock timeout"):
                Storage.objects.create(
                    name="Blocked storage", contributor=self.contributor, json={}
                )
            self.assertLess(time() - start, 10)
        finally:
            release.cancel()
            blocker.rollback()
            blocker.close()

    @override_settings(
        LISTENER_DATABASE_LOCK_TIMEOUT=1, LISTENER_DATABASE_WRITE_ATTEMPTS=4
    )
    def test_blocked_write_is_retried(self):
        """A write aborted by the database is repeated once the lock is gone."""
        enable_database_timeouts()
        connection.close()

        blocker = connection.copy()
        blocker.set_autocommit(False)
        user_table = connection.ops.quote_name(get_user_model()._meta.db_table)
        with blocker.cursor() as cursor:
            cursor.execute(
                f"SELECT id FROM {user_table} WHERE id = %s FOR UPDATE",
                [self.contributor.pk],
            )

        attempts = []

        @retry_database_writes
        def create_storage():
            attempts.append(len(attempts))
            return Storage.objects.create(
                name="Blocked storage", contributor=self.contributor, json={}
            )

        # The lock is released while the retry sleeps, so the first attempt
        # always hits it and the second one never does.
        release = patch(
            "resolwe.flow.managers.listener.database.time.sleep",
            side_effect=lambda seconds: blocker.rollback(),
        )
        try:
            with release:
                storage = create_storage()
        finally:
            blocker.rollback()
            blocker.close()

        self.assertEqual(len(attempts), 2)
        self.assertTrue(Storage.objects.filter(pk=storage.pk).exists())

    @override_settings(LISTENER_DATABASE_LOCK_TIMEOUT=1)
    def test_download_started_rereads_the_claimed_location(self):
        """A repeated download start sees the claim made while it waited."""
        enable_database_timeouts()
        connection.close()
        location = StorageLocation.objects.create(
            file_storage=FileStorage.objects.create(),
            connector_name="local",
            status=StorageLocation.STATUS_PREPARING,
        )
        table = connection.ops.quote_name(StorageLocation._meta.db_table)

        blocker = connection.copy()
        blocker.set_autocommit(False)
        with blocker.cursor() as cursor:
            cursor.execute(
                f"SELECT id FROM {table} WHERE id = %s FOR UPDATE", [location.pk]
            )

        def claim_location(seconds):
            """Claim the location on the blocking connection and release it."""
            with blocker.cursor() as cursor:
                cursor.execute(
                    f"UPDATE {table} SET status = %s WHERE id = %s",
                    [StorageLocation.STATUS_UPLOADING, location.pk],
                )
            blocker.commit()

        message = Message.command(
            "download_started",
            {
                ExecutorProtocol.STORAGE_LOCATION_ID: location.pk,
                ExecutorProtocol.DOWNLOAD_STARTED_LOCK: True,
            },
            client_id=b"0",
        )
        claim = patch(
            "resolwe.flow.managers.listener.database.time.sleep",
            side_effect=claim_location,
        )
        try:
            with claim as sleep:
                response = BasicCommands().handle_download_started(
                    1, message, MagicMock()
                )
        finally:
            blocker.rollback()
            blocker.close()

        sleep.assert_called_once()
        self.assertEqual(response.message_data, ExecutorProtocol.DOWNLOAD_IN_PROGRESS)
        location.refresh_from_db()
        self.assertEqual(location.status, StorageLocation.STATUS_UPLOADING)


class DatabaseDriverError(Exception):
    """Stand-in for the database driver error Django wraps."""

    def __init__(self, sqlstate: str):
        """Remember the sqlstate of the error."""
        super().__init__(sqlstate)
        self.sqlstate = sqlstate


class ListenerDatabaseWriteTest(TransactionTestCase):
    """Test the repeated database writes of the listener."""

    def _database_error(self, sqlstate: str) -> OperationalError:
        """Return a database error with the given sqlstate."""
        error = OperationalError("aborted")
        error.__cause__ = DatabaseDriverError(sqlstate)
        return error

    def _failing(self, error: Exception, failures: int):
        """Return a function failing the given number of times."""
        calls = []

        def failing():
            calls.append(len(calls))
            if len(calls) <= failures:
                raise error
            return "done"

        failing.calls = calls
        return failing

    def test_retriable_errors(self):
        """Only the errors that abort the transaction are retried."""
        for sqlstate in ("57014", "55P03", "40001", "40P01"):
            with self.subTest(sqlstate=sqlstate):
                self.assertTrue(
                    is_retriable_database_error(self._database_error(sqlstate))
                )
        # Unique violation: the write itself is wrong, repeating it can not
        # help.
        self.assertFalse(is_retriable_database_error(self._database_error("23505")))
        # Connection errors are not retried: the outcome is unknown.
        self.assertFalse(is_retriable_database_error(self._database_error("08006")))
        self.assertFalse(is_retriable_database_error(OperationalError("no cause")))

    @patch("resolwe.flow.managers.listener.database.random.uniform", return_value=1.0)
    @patch("resolwe.flow.managers.listener.database.time.sleep")
    def test_retries_until_success(self, sleep, uniform):
        """The write is repeated until it succeeds."""
        failing = self._failing(self._database_error("57014"), failures=2)
        self.assertEqual(retry_database_writes(failing)(), "done")
        self.assertEqual(len(failing.calls), 3)
        # The sleep between the attempts is doubled every time and spread by a
        # random factor.
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [1, 2])
        uniform.assert_called_with(0.5, 1.5)

    @patch("resolwe.flow.managers.listener.database.time.sleep")
    def test_gives_up_after_all_attempts(self, sleep):
        """The error of the last attempt is raised when all attempts fail."""
        failing = self._failing(self._database_error("57014"), failures=100)
        with self.assertRaisesMessage(OperationalError, "aborted"):
            retry_database_writes(failing)()
        self.assertEqual(len(failing.calls), DEFAULT_DATABASE_WRITE_ATTEMPTS)

    @override_settings(LISTENER_DATABASE_WRITE_ATTEMPTS=2)
    @patch("resolwe.flow.managers.listener.database.time.sleep")
    def test_attempts_from_settings(self, sleep):
        """The number of attempts is read from the settings."""
        failing = self._failing(self._database_error("57014"), failures=100)
        with self.assertRaisesMessage(OperationalError, "aborted"):
            retry_database_writes(failing)()
        self.assertEqual(len(failing.calls), 2)

    @patch("resolwe.flow.managers.listener.database.time.sleep")
    def test_other_errors_are_not_retried(self, sleep):
        """An error that repeating can not fix is raised immediately."""
        failing = self._failing(self._database_error("23505"), failures=100)
        with self.assertRaisesMessage(OperationalError, "aborted"):
            retry_database_writes(failing)()
        self.assertEqual(len(failing.calls), 1)
        sleep.assert_not_called()

    def test_create_object_is_atomic(self):
        """The created object and its side effects share one transaction."""
        in_atomic_block = []

        def create(**kwargs):
            in_atomic_block.append(connection.in_atomic_block)
            return SimpleNamespace(id=1)

        manager = MagicMock()
        manager.contributor.return_value = self.contributor
        message = Message.command(
            "create_object", ("flow", "Storage", {"json": {}}), client_id=b"0"
        )
        with (
            patch.object(Storage.objects, "create", side_effect=create),
            patch.object(permission_manager, "can_create"),
        ):
            response = PythonProcess().handle_create_object(1, message, manager)

        self.assertEqual(response.message_data, 1)
        self.assertEqual(in_atomic_block, [True])

    @patch("resolwe.flow.managers.listener.database.time.sleep")
    def test_finish_is_retried(self, sleep):
        """The final write of a finished data object is repeated as a whole."""
        data = SimpleNamespace(status="PR", process_error=[], location=MagicMock())
        manager = MagicMock()
        manager.data.return_value = data
        manager._save_data.side_effect = [self._database_error("57014"), None]

        message = Message.command("finish", {"rc": 1}, client_id=b"0")
        response = BasicCommands().handle_finish(1, message, manager)

        self.assertEqual(response.message_data, "OK")
        self.assertEqual(manager._save_data.call_count, 2)
        # The worker is updated only in the attempt that succeeded.
        manager._update_worker.assert_called_once()

    @patch("resolwe.flow.managers.listener.database.time.sleep")
    def test_update_status_is_retried(self, sleep):
        """The status update of a data object is repeated as a whole."""
        data = SimpleNamespace(status=Data.STATUS_WAITING)
        manager = MagicMock()
        manager.get_data_fields.return_value = Data.STATUS_WAITING
        manager._choose_worst_status.return_value = Data.STATUS_PROCESSING
        manager.data.return_value = data
        manager._save_data.side_effect = [self._database_error("57014"), None]

        message = Message.command(
            "update_status", Data.STATUS_PROCESSING, client_id=b"0"
        )
        response = BasicCommands().handle_update_status(1, message, manager)

        self.assertEqual(response.message_data, Data.STATUS_PROCESSING)
        self.assertEqual(manager._save_data.call_count, 2)
        # The worker is updated only in the attempt that succeeded.
        manager._update_worker.assert_called_once_with(
            1, changes={"status": Worker.STATUS_PROCESSING}
        )

    @patch("resolwe.flow.managers.listener.database.time.sleep")
    def test_update_output_restores_output(self, sleep):
        """A repeated output update starts from the stored output."""
        data = SimpleNamespace(pk=1, id=1, output={})
        outputs_seen = []
        storage_pks = iter([41, 51, 52])

        def save_storage(key, value, data_object):
            outputs_seen.append(deepcopy(data_object.output))
            # The first attempt is aborted after the first storage was created.
            if len(outputs_seen) == 2:
                raise self._database_error("57014")
            return SimpleNamespace(pk=next(storage_pks))

        manager = MagicMock()
        manager.data.return_value = data
        manager.get_data_fields.return_value = [
            {"name": "first", "type": "basic:json:", "label": "First"},
            {"name": "second", "type": "basic:json:", "label": "Second"},
        ]
        manager.save_storage.side_effect = save_storage

        message = Message.command(
            "update_output", {"first": {"a": 1}, "second": {"b": 2}}, client_id=b"0"
        )
        BasicCommands().handle_update_output(data.pk, message, manager)

        self.assertEqual(outputs_seen, [{}, {"first": 41}, {}, {"first": 51}])
        self.assertEqual(data.output, {"first": 51, "second": 52})
