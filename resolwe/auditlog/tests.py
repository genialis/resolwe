# pylint: disable=missing-docstring
"""Tests for the audit log context handling."""

from asgiref.sync import async_to_sync
from channels.db import database_sync_to_async
from django.test import SimpleTestCase

from resolwe.auditlog.auditmanager import AccessType, AuditManager, audit_context
from resolwe.flow.models import Data

# The field values needed to materialize a Data instance without hitting the
# database: the constructor reads the name and output fields.
DATA_FIELDS = ["id", "name", "output", "status"]
DATA_VALUES = [7, "test-data", {}, Data.STATUS_DONE]


class AuditContextTest(SimpleTestCase):
    def test_no_accumulation_without_context(self):
        """Accesses outside an audit context must be discarded."""
        self.assertIsNone(AuditManager.current())
        # Must not raise and must not activate a manager.
        AuditManager.register_access(Data, 1, AccessType.READ, {"id"})
        AuditManager.log_message("A message outside the context.")
        self.assertIsNone(AuditManager.current())

    def test_context_activation(self):
        """The manager is only active inside the context."""
        self.assertIsNone(AuditManager.current())
        with audit_context() as manager:
            self.assertIs(AuditManager.current(), manager)
        self.assertIsNone(AuditManager.current())

    def test_collect_and_emit(self):
        """Accesses inside the context are emitted with the context data."""
        with self.assertLogs("auditlog") as captured:
            with audit_context(user_id=42, context_id="unit-test"):
                # Django calls from_db when materializing every queryset row.
                Data.from_db("default", DATA_FIELDS, DATA_VALUES)
                AuditManager.log_message("Custom %s.", "message")

        access_records = [
            record
            for record in captured.records
            if record.getMessage().startswith("Object accessed:")
        ]
        self.assertEqual(len(access_records), 1)
        access_record = access_records[0]
        self.assertIn("flow.Data 7", access_record.getMessage())
        self.assertIn("READ", access_record.getMessage())
        self.assertEqual(access_record.user_id, 42)
        self.assertEqual(access_record.request_id, "unit-test")

        message_records = [
            record
            for record in captured.records
            if record.getMessage() == "Custom message."
        ]
        self.assertEqual(len(message_records), 1)
        self.assertEqual(message_records[0].user_id, 42)
        self.assertEqual(message_records[0].request_id, "unit-test")

    def test_emit_on_exception(self):
        """Accesses are emitted also when the unit of work fails."""
        with self.assertLogs("auditlog") as captured:
            with self.assertRaises(RuntimeError):
                with audit_context(context_id="failing-unit"):
                    Data.from_db("default", DATA_FIELDS, DATA_VALUES)
                    raise RuntimeError("The unit of work failed.")
        self.assertIn("flow.Data 7", captured.output[0])

    def test_auto_emit_disabled(self):
        """Nothing is emitted automatically when auto_emit is disabled."""
        with self.assertNoLogs("auditlog"):
            with audit_context(auto_emit=False) as manager:
                Data.from_db("default", DATA_FIELDS, DATA_VALUES)
        # The caller is responsible for emitting the collected data.
        with self.assertLogs("auditlog") as captured:
            manager.emit(extra={"request_id": "manual-emit"})
        self.assertIn("flow.Data 7", captured.output[0])

    def test_deduplication(self):
        """Repeated accesses to the same object emit a single record."""
        with self.assertLogs("auditlog") as captured:
            with audit_context():
                for _ in range(10):
                    Data.from_db("default", DATA_FIELDS, DATA_VALUES)
        access_records = [
            record
            for record in captured.records
            if record.getMessage().startswith("Object accessed:")
        ]
        self.assertEqual(len(access_records), 1)

    def test_context_active_in_database_thread(self):
        """The context propagates into (database_)sync_to_async threads."""

        def register():
            AuditManager.register_access(Data, 1, AccessType.READ, {"id"})

        async def unit_of_work():
            with audit_context(context_id="thread-test"):
                await database_sync_to_async(register, thread_sensitive=False)()

        with self.assertLogs("auditlog") as captured:
            async_to_sync(unit_of_work)()
        self.assertIn("flow.Data 1", captured.output[0])
        self.assertEqual(captured.records[0].request_id, "thread-test")
