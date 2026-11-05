# pylint: disable=missing-docstring
import unittest
from unittest.mock import MagicMock, patch

from resolwe.process.communicator import CommandError
from resolwe.process.runtime import Process, ProcessError


class ProcessStartTest(unittest.TestCase):
    """Test how ``Process.start`` reports errors to the listener."""

    def start(self, run, process_log=None) -> list:
        """Start a process with the given ``run`` method.

        :return: the arguments of the ``process_log`` commands sent.
        """
        process_class = type("TestProcess", (Process,), {"_abstract": True, "run": run})
        process = process_class(MagicMock())
        with (
            patch("resolwe.process.runtime.communicator") as communicator,
            self.assertLogs("TestProcess", level="ERROR"),
        ):
            if process_log is not None:
                communicator.process_log.side_effect = process_log
            try:
                process.start()
            finally:
                self.sent = [
                    call.args[0] for call in communicator.process_log.call_args_list
                ]
        return self.sent

    def test_error_stops_process(self):
        reached = []

        def run(self, inputs, outputs):
            self.error("Wrong", "input")
            reached.append(True)

        with self.assertRaisesRegex(ProcessError, "^Wrong input$"):
            self.start(run)
        self.assertEqual(reached, [])
        self.assertEqual(self.sent, [{"error": ["Wrong input"]}])

    def test_error_stops_process_when_log_is_rejected(self):
        def run(self, inputs, outputs):
            self.error("Wrong input")

        rejected = CommandError("Command 'process_log' failed: rejected")
        with self.assertRaisesRegex(ProcessError, "^Wrong input$"):
            self.start(run, process_log=rejected)
        self.assertEqual(self.sent, [{"error": ["Wrong input"]}])

    def test_raised_process_error_reported_once(self):
        def run(self, inputs, outputs):
            raise ProcessError("Raised directly")

        with self.assertRaisesRegex(ProcessError, "^Raised directly$"):
            self.start(run)
        self.assertEqual(self.sent, [{"error": ["Raised directly"]}])

    def test_exception_reported_once(self):
        def run(self, inputs, outputs):
            raise KeyError("missing")

        with self.assertRaises(KeyError):
            self.start(run)
        self.assertEqual(self.sent, [{"error": ["'missing'"]}])

    def test_base_exception_propagates(self):
        def run(self, inputs, outputs):
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            self.start(run)
        self.assertEqual(self.sent, [{"error": ["Exception while running process"]}])

    def test_process_error_escapes_narrow_handlers(self):
        # Processes call ``error`` inside ``try`` blocks that handle such
        # exceptions; the handlers must not catch the reported error.
        for exception_class in (KeyError, ValueError, RuntimeError):
            self.assertFalse(issubclass(ProcessError, exception_class))
