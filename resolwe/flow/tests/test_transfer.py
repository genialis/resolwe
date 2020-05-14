# pylint: disable=missing-docstring
import asyncio
import json
import sys
from functools import partial
from unittest.mock import MagicMock, call, patch

from resolwe.flow import executors
from resolwe.flow.managers.protocol import ExecutorProtocol
from resolwe.flow.models import Data
from resolwe.storage import connectors
from resolwe.storage.connectors.exceptions import DataTransferError
from resolwe.test import TestCase


async def coroutine(*args, **kwargs):
    """Coroutine that just returns its first argument."""
    return args[0]


async def send(rules, command, extra_fields=None, expect_reply=True):
    """Coroutine used to mock send_manager_command. It accepts the list of
    expected calls and replies. When the actual call deviates from these
    list the message with status ExecutorProtocol.RESULT_ERROR is sent to the
    executor.
    """
    rule_index = rules[0]
    rules[0] += 1
    expected, response = rules[rule_index]
    if [command, extra_fields] != expected:
        return {ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_ERROR}
    else:
        if callable(response):
            return response()
        else:
            return response


async def start(actual_coroutine):
    """
    Start the testing coroutine and wait 1 second for it to complete.
    :raises asyncio.CancelledError when the coroutine fails to finish its work
        in 1 second.
    :returns: the return value of the actual_coroutine.
    :rtype: Any

    """
    try:
        return await asyncio.wait_for(actual_coroutine, 2)
    except asyncio.CancelledError:
        pass


def run_async(coroutine):
    loop = asyncio.new_event_loop()
    return loop.run_until_complete(start(coroutine))


def raise_datatransfererror():
    raise DataTransferError()


class SettingsJSONifier(json.JSONEncoder):
    """Customized JSON encoder, coercing all unknown types into strings.

    Needed due to the class hierarchy coming out of the database,
    which can't be serialized using the vanilla json encoder.
    """

    def default(self, o):
        """Try default; otherwise, coerce the object into a string."""
        try:
            return super().default(o)
        except TypeError:
            return str(o)


MODULES_PATCH = {"resolwe.flow.executors.connectors": connectors, "sphinx": MagicMock()}
TRANSFER = "resolwe.flow.executors.transfer"
TRANSFER_PATCHES = {
    "DATA_META": {
        k: getattr(Data, k)
        for k in dir(Data)
        if k.startswith("STATUS_") and isinstance(getattr(Data, k), str)
    }
}


class BasicTestCase(TestCase):
    RESULT_ERROR = {ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_ERROR}
    RESULT_OK = {ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_OK}
    DOWNLOAD_STARTED = {
        ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_OK,
        ExecutorProtocol.DOWNLOAD_RESULT: ExecutorProtocol.DOWNLOAD_STARTED,
    }
    DOWNLOAD_IN_PROGRESS = {
        ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_OK,
        ExecutorProtocol.DOWNLOAD_RESULT: ExecutorProtocol.DOWNLOAD_IN_PROGRESS,
    }
    DOWNLOAD_FINISHED = {
        ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_OK,
        ExecutorProtocol.DOWNLOAD_RESULT: ExecutorProtocol.DOWNLOAD_FINISHED,
    }
    ACCESS_LOG = {
        ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_OK,
        ExecutorProtocol.STORAGE_ACCESS_LOG_ID: 1,
    }


@patch.dict(sys.modules, MODULES_PATCH)
@patch.multiple(TRANSFER, **TRANSFER_PATCHES)
class TransfersTest(BasicTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        self.missing_data = [
            {
                "connector_name": "CONNECTOR",
                "url": "URL",
                "data_id": "1",
                "from_storage_location_id": 1,
                "to_storage_location_id": 2,
            }
        ]
        self.DOWNLOAD_STARTED_LOCK = [
            ExecutorProtocol.DOWNLOAD_STARTED,
            {
                ExecutorProtocol.STORAGE_LOCATION_ID: 2,
                ExecutorProtocol.DOWNLOAD_STARTED_LOCK: True,
            },
        ]
        self.DOWNLOAD_STARTED_NO_LOCK = [
            ExecutorProtocol.DOWNLOAD_STARTED,
            {
                ExecutorProtocol.STORAGE_LOCATION_ID: 2,
                ExecutorProtocol.DOWNLOAD_STARTED_LOCK: False,
            },
        ]
        self.MISSING_DATA = [
            ExecutorProtocol.MISSING_DATA_LOCATIONS,
            None,
        ]
        self.MISSING_DATA_RESPONSE = {
            ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_OK,
            ExecutorProtocol.STORAGE_DATA_LOCATIONS: self.missing_data.copy(),
        }

        return super().setUp()

    def _test_workflow(self, send_command, download_command, commands, result):
        with patch.multiple(
            TRANSFER, **self.get_patches(send_command, download_command)
        ):
            result = run_async(executors.transfer._transfer_data())
        self.assertEqual(send_command.call_count, len(commands) - 1)
        expected_args_list = []
        for data, _ in commands[1:]:
            command, extra_fields = data
            if extra_fields is not None:
                expected_args_list.append(call(command, extra_fields=extra_fields))
            else:
                expected_args_list.append(call(command))
        self.assertEqual(expected_args_list, send_command.call_args_list)
        return result

    def get_patches(self, send_command, download_command=None):
        patches = {
            "send_manager_command": send_command,
            "DOWNLOAD_WAITING_TIMEOUT": 0,
        }
        if download_command is not None:
            patches["download_data"] = download_command
        return patches

    def test_no_transfer(self):
        self.MISSING_DATA_RESPONSE = {
            ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_OK,
            ExecutorProtocol.STORAGE_DATA_LOCATIONS: [],
        }
        send_command = MagicMock(
            side_effect=[
                coroutine(self.MISSING_DATA_RESPONSE),
                coroutine(self.RESULT_OK),
            ]
        )
        with patch.multiple(TRANSFER, send_manager_command=send_command):
            run_async(executors.transfer._transfer_data())
        send_command.assert_called_once_with("missing_data_locations")

    def test_transfer_download(self):
        commands = [
            1,
            (self.MISSING_DATA, self.MISSING_DATA_RESPONSE),
            (["update", {"changeset": {"status": "PP"}}], self.RESULT_OK),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_STARTED),
        ]
        send_command = MagicMock(side_effect=partial(send, commands))
        download_command = MagicMock(return_value=coroutine(True))
        result = self._test_workflow(send_command, download_command, commands, True)
        self.assertTrue(result)
        download_command.assert_called_once_with(self.missing_data[0])

    def test_transfer_wait_download(self):
        commands = [
            1,
            (self.MISSING_DATA, self.MISSING_DATA_RESPONSE),
            (["update", {"changeset": {"status": "PP"}}], self.RESULT_OK),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_IN_PROGRESS),
            (self.DOWNLOAD_STARTED_NO_LOCK, self.DOWNLOAD_STARTED),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_STARTED),
        ]
        download_command = MagicMock(return_value=coroutine(True))
        send_command = MagicMock(side_effect=partial(send, commands))
        result = self._test_workflow(send_command, download_command, commands, True)
        self.assertTrue(result)
        download_command.assert_called_once_with(self.missing_data[0])

    def test_transfer_wait_download_long(self):
        commands = [
            1,
            (self.MISSING_DATA, self.MISSING_DATA_RESPONSE),
            (["update", {"changeset": {"status": "PP"}}], self.RESULT_OK),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_IN_PROGRESS),
            (self.DOWNLOAD_STARTED_NO_LOCK, self.DOWNLOAD_STARTED),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_IN_PROGRESS),
            (self.DOWNLOAD_STARTED_NO_LOCK, self.DOWNLOAD_IN_PROGRESS),
            (self.DOWNLOAD_STARTED_NO_LOCK, self.DOWNLOAD_STARTED),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_IN_PROGRESS),
            (self.DOWNLOAD_STARTED_NO_LOCK, self.DOWNLOAD_IN_PROGRESS),
            (self.DOWNLOAD_STARTED_NO_LOCK, self.DOWNLOAD_STARTED),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_STARTED),
        ]
        download_command = MagicMock(return_value=coroutine(True))
        send_command = MagicMock(side_effect=partial(send, commands))
        result = self._test_workflow(send_command, download_command, commands, True)
        self.assertTrue(result)
        download_command.assert_called_once_with(self.missing_data[0])

    def test_transfer_failed(self):
        commands = [
            1,
            (self.MISSING_DATA, self.MISSING_DATA_RESPONSE),
            (["update", {"changeset": {"status": "PP"}}], self.RESULT_OK),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_STARTED),
        ]
        download_command = MagicMock(return_value=coroutine(False))
        send_command = MagicMock(side_effect=partial(send, commands))
        result = self._test_workflow(send_command, download_command, commands, True)
        self.assertFalse(result)
        download_command.assert_called_once_with(self.missing_data[0])

    def test_transfer_downloaded(self):
        commands = [
            1,
            (self.MISSING_DATA, self.MISSING_DATA_RESPONSE),
            (["update", {"changeset": {"status": "PP"}}], self.RESULT_OK),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_FINISHED),
        ]
        download_command = MagicMock(return_value=coroutine(True))
        send_command = MagicMock(side_effect=partial(send, commands))
        result = self._test_workflow(send_command, download_command, commands, True)
        self.assertTrue(result)
        download_command.assert_not_called()

    def test_transfer_wait_downloaded(self):
        commands = [
            1,
            (self.MISSING_DATA, self.MISSING_DATA_RESPONSE),
            (["update", {"changeset": {"status": "PP"}}], self.RESULT_OK),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_IN_PROGRESS),
            (self.DOWNLOAD_STARTED_NO_LOCK, self.DOWNLOAD_FINISHED),
        ]
        download_command = MagicMock(return_value=coroutine(True))
        send_command = MagicMock(side_effect=partial(send, commands))
        result = self._test_workflow(send_command, download_command, commands, True)
        self.assertTrue(result)
        download_command.assert_not_called()

    def test_transfer_downloadmulti_simple(self):
        self.missing_data = [
            {
                "connector_name": "CONNECTOR",
                "url": "URL",
                "data_id": "1",
                "from_storage_location_id": 1,
                "to_storage_location_id": 2,
            },
            {
                "connector_name": "CONNECTOR",
                "url": "URL",
                "data_id": "1",
                "from_storage_location_id": 1,
                "to_storage_location_id": 2,
            },
        ]
        self.MISSING_DATA_RESPONSE = {
            ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_OK,
            ExecutorProtocol.STORAGE_DATA_LOCATIONS: self.missing_data.copy(),
        }
        commands = [
            1,
            (self.MISSING_DATA, self.MISSING_DATA_RESPONSE),
            (["update", {"changeset": {"status": "PP"}}], self.RESULT_OK),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_STARTED),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_STARTED),
        ]
        download_command = MagicMock(side_effect=lambda e: coroutine(True))
        send_command = MagicMock(side_effect=partial(send, commands))
        result = self._test_workflow(send_command, download_command, commands, True)
        self.assertTrue(result)
        self.assertEqual(download_command.call_count, 2)

    def test_transfer_downloadmulti(self):
        self.missing_data = [
            {
                "connector_name": "CONNECTOR",
                "url": "URL",
                "data_id": "1",
                "from_storage_location_id": 1,
                "to_storage_location_id": 2,
            },
            {
                "connector_name": "CONNECTOR",
                "url": "URL",
                "data_id": "1",
                "from_storage_location_id": 1,
                "to_storage_location_id": 2,
            },
        ]
        self.MISSING_DATA_RESPONSE = {
            ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_OK,
            ExecutorProtocol.STORAGE_DATA_LOCATIONS: self.missing_data.copy(),
        }
        commands = [
            1,
            (self.MISSING_DATA, self.MISSING_DATA_RESPONSE),
            (["update", {"changeset": {"status": "PP"}}], self.RESULT_OK),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_IN_PROGRESS),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_STARTED),
            (self.DOWNLOAD_STARTED_NO_LOCK, self.DOWNLOAD_FINISHED),
        ]
        download_command = MagicMock(side_effect=lambda e: coroutine(True))
        send_command = MagicMock(side_effect=partial(send, commands))
        result = self._test_workflow(send_command, download_command, commands, True)
        self.assertTrue(result)
        download_command.assert_called_once()

    def test_transfer_protocol_fail(self):
        async def raise_exception(*args, **kwargs):
            raise RuntimeError("Protocol error")

        commands = [
            1,
            (self.MISSING_DATA, self.MISSING_DATA_RESPONSE),
            (["update", {"changeset": {"status": "PP"}}], self.RESULT_OK),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_STARTED),
        ]
        download_command = MagicMock(return_value=coroutine(True))
        send_command = MagicMock(side_effect=raise_exception)
        with self.assertRaises(RuntimeError):
            self._test_workflow(send_command, download_command, commands, True)
        download_command.assert_not_called()

    def test_outer_command_success(self):
        send_manager_command = MagicMock()
        _transfer_data = MagicMock(return_value=coroutine(True))
        patches = {
            "_transfer_data": _transfer_data,
            "send_manager_command": send_manager_command,
        }
        with patch.multiple(TRANSFER, **patches):
            result = run_async(executors.transfer.transfer_data())
        self.assertIsNone(result)
        _transfer_data.assert_called_once()
        send_manager_command.assert_not_called()

    def test_outer_command_failure(self):
        send_manager_command = MagicMock()
        commands = [
            1,
            (
                [
                    "update",
                    {
                        "changeset": {
                            "process_error": ["Failed to transfer data."],
                            "status": Data.STATUS_ERROR,
                        }
                    },
                ],
                self.RESULT_OK,
            ),
            (["abort", {}], self.RESULT_OK),
        ]
        send_manager_command = MagicMock(side_effect=partial(send, commands))

        _transfer_data = MagicMock(return_value=coroutine(False))
        patches = {
            "_transfer_data": _transfer_data,
            "send_manager_command": send_manager_command,
        }
        with patch.multiple(TRANSFER, **patches):
            with self.assertRaises(SystemExit) as context_manager:
                run_async(executors.transfer.transfer_data())
        self.assertEqual(context_manager.exception.code, 1)
        _transfer_data.assert_called_once()
        calls = [
            call(commands[1][0][0], extra_fields=commands[1][0][1]),
            call(commands[2][0][0], expect_reply=False),
        ]
        self.assertEqual(send_manager_command.call_args_list, calls)


@patch.dict(sys.modules, MODULES_PATCH)
@patch.multiple(TRANSFER, **TRANSFER_PATCHES)
class DownloadDataTest(BasicTestCase):
    def setUp(self):
        self.missing_data = {
            "connector_name": "S3",
            "url": "transfer_url",
            "data_id": 1,
            "from_storage_location_id": 1,
            "to_storage_location_id": 2,
        }
        self.COMMAND_LOCATION_LOCK = [
            ExecutorProtocol.STORAGE_LOCATION_LOCK,
            {
                ExecutorProtocol.STORAGE_LOCATION_ID: 1,
                ExecutorProtocol.STORAGE_LOCATION_LOCK_REASON: "Executor data transfer",
            },
        ]
        self.COMMAND_DOWNLOAD_FINISHED = [
            ExecutorProtocol.DOWNLOAD_FINISHED,
            {ExecutorProtocol.STORAGE_LOCATION_ID: 2},
        ]
        self.COMMAND_DOWNLOAD_ABORTED = [
            ExecutorProtocol.DOWNLOAD_ABORTED,
            {ExecutorProtocol.STORAGE_LOCATION_ID: 2},
        ]
        self.COMMAND_LOCATION_UNLOCK = [
            ExecutorProtocol.STORAGE_LOCATION_UNLOCK,
            {ExecutorProtocol.STORAGE_ACCESS_LOG_ID: 1},
        ]
        self.COMMAND_GET_FILES = [
            ExecutorProtocol.GET_FILES_TO_DOWNLOAD,
            {ExecutorProtocol.STORAGE_LOCATION_ID: 1},
        ]
        self.FILES_LIST = {
            ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_OK,
            ExecutorProtocol.REFERENCED_FILES: ["1", "dir/1"],
        }

        return super().setUp()

    def get_patches(self, send_command, transfer_module):
        patches = {
            "send_manager_command": send_command,
            "Transfer": transfer_module,
            "DataTransferError": DataTransferError,
            "RETRIES": 2,
        }
        return patches

    def _test_workflow_calls(self, send_command, commands):
        self.assertEqual(send_command.call_count, len(commands) - 1)
        expected_args_list = []
        for i in range(1, len(commands)):
            command, extra_fields = commands[i][0]
            kwargs = {"extra_fields": extra_fields}
            if command in (
                ExecutorProtocol.STORAGE_LOCATION_UNLOCK,
                ExecutorProtocol.DOWNLOAD_ABORTED,
            ):
                kwargs["expect_reply"] = False
            expected_args_list.append(call(command, **kwargs))
        self.assertEqual(expected_args_list, send_command.call_args_list)

    def _test_workflow(
        self, send_command, transfer_module, commands, expected_result=True
    ):
        with patch.multiple(
            TRANSFER, **self.get_patches(send_command, transfer_module),
        ):
            result = run_async(
                executors.transfer.download_data(self.missing_data.copy())
            )
        self.assertEqual(result, expected_result)
        self._test_workflow_calls(send_command, commands)

    def test_download_simple(self):
        commands = [
            1,
            (self.COMMAND_LOCATION_LOCK, self.ACCESS_LOG),
            (self.COMMAND_GET_FILES, self.FILES_LIST),
            (self.COMMAND_DOWNLOAD_FINISHED, self.RESULT_OK),
            (self.COMMAND_LOCATION_UNLOCK, self.RESULT_OK),
        ]
        send_command = MagicMock(side_effect=partial(send, commands))

        # Mock the transfer object
        transfer_objects = MagicMock()
        transfer = MagicMock(transfer_objects=transfer_objects)
        transfer_module = MagicMock(return_value=transfer)
        self._test_workflow(send_command, transfer_module, commands)
        transfer_objects.assert_called_once_with("transfer_url", ["1", "dir/1"])

    def test_download_retry(self):
        commands = [
            1,
            (self.COMMAND_LOCATION_LOCK, self.ACCESS_LOG),
            (self.COMMAND_GET_FILES, self.FILES_LIST),
            (self.COMMAND_DOWNLOAD_FINISHED, raise_datatransfererror),
            (self.COMMAND_LOCATION_UNLOCK, self.RESULT_OK),
            (self.COMMAND_LOCATION_LOCK, self.ACCESS_LOG),
            (self.COMMAND_DOWNLOAD_FINISHED, self.RESULT_OK),
            (self.COMMAND_LOCATION_UNLOCK, self.RESULT_OK),
        ]
        send_command = MagicMock(side_effect=partial(send, commands))

        # Mock the transfer object
        transfer_objects = MagicMock()
        transfer = MagicMock(transfer_objects=transfer_objects)
        transfer_module = MagicMock(return_value=transfer)
        self._test_workflow(send_command, transfer_module, commands)
        self.assertEqual(
            transfer_objects.call_args_list, [call("transfer_url", ["1", "dir/1"])] * 2,
        )

    def test_download_retry_fail(self):
        commands = [
            1,
            (self.COMMAND_LOCATION_LOCK, self.ACCESS_LOG),
            (self.COMMAND_GET_FILES, self.FILES_LIST),
            (self.COMMAND_DOWNLOAD_FINISHED, raise_datatransfererror),
            (self.COMMAND_LOCATION_UNLOCK, self.RESULT_OK),
            (self.COMMAND_LOCATION_LOCK, self.ACCESS_LOG),
            (self.COMMAND_DOWNLOAD_FINISHED, raise_datatransfererror),
            (self.COMMAND_LOCATION_UNLOCK, self.RESULT_OK),
            (self.COMMAND_DOWNLOAD_ABORTED, self.RESULT_OK),
        ]
        send_command = MagicMock(side_effect=partial(send, commands))

        # Mock the transfer object
        transfer_objects = MagicMock()
        transfer = MagicMock(transfer_objects=transfer_objects)
        transfer_module = MagicMock(return_value=transfer)

        self._test_workflow(send_command, transfer_module, commands, False)
        self.assertEqual(
            transfer_objects.call_args_list, [call("transfer_url", ["1", "dir/1"])] * 2,
        )
