# pylint: disable=missing-docstring
import asyncio
import json
import sys
from functools import partial
from unittest.mock import MagicMock, call, patch

from resolwe.flow.executors import transfer
from resolwe.flow.executors.socket_utils import Message, Response, ResponseStatus
from resolwe.flow.managers.protocol import ExecutorProtocol
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
    if command != expected:
        return Response(ResponseStatus.ERROR.value, "ERROR")
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


class BasicTestCase(TestCase):
    RESULT_ERROR = {ExecutorProtocol.RESULT: ExecutorProtocol.RESULT_ERROR}
    RESULT_OK = Response(ResponseStatus.OK.value, "OK")
    DOWNLOAD_STARTED = Response(
        ResponseStatus.OK.value, ExecutorProtocol.DOWNLOAD_STARTED
    )
    DOWNLOAD_IN_PROGRESS = Response(
        ResponseStatus.OK.value, ExecutorProtocol.DOWNLOAD_IN_PROGRESS
    )
    DOWNLOAD_FINISHED = Response(
        ResponseStatus.OK.value, ExecutorProtocol.DOWNLOAD_FINISHED
    )
    ACCESS_LOG = Response(ResponseStatus.OK.value, 1)


@patch.dict(sys.modules, MODULES_PATCH)
class TransfersTest(BasicTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setUp(self):
        self.communicator_mock = MagicMock()
        self.missing_data = [
            {
                "connector_name": "CONNECTOR",
                "url": "URL",
                "data_id": "1",
                "from_storage_location_id": 1,
                "to_storage_location_id": 2,
                "to_connector": "local",
            }
        ]
        self.DOWNLOAD_STARTED_LOCK = Message.command(
            "download_started",
            {
                ExecutorProtocol.STORAGE_LOCATION_ID: 2,
                ExecutorProtocol.DOWNLOAD_STARTED_LOCK: True,
            },
        )
        self.DOWNLOAD_STARTED_NO_LOCK = Message.command(
            "download_started",
            {
                ExecutorProtocol.STORAGE_LOCATION_ID: 2,
                ExecutorProtocol.DOWNLOAD_STARTED_LOCK: False,
            },
        )
        self.MISSING_DATA = Message.command(ExecutorProtocol.MISSING_DATA_LOCATIONS, "")

        self.MISSING_DATA_RESPONSE = Response(
            ResponseStatus.OK.value, self.missing_data.copy()
        )

        return super().setUp()

    def _test_workflow(
        self, send_command, download_command, commands, result, missing_data
    ):
        self.communicator_mock.send_command = send_command
        with patch.multiple(
            TRANSFER,
            **self.get_patches(send_command, download_command),
        ):
            result = run_async(
                transfer.transfer_data(self.communicator_mock, missing_data)
            )
        self.assertEqual(send_command.call_count, len(commands) - 1)
        expected_args_list = []
        for message, _ in commands[1:]:
            expected_args_list.append(call(message))
        self.assertEqual(expected_args_list, send_command.call_args_list)
        return result

    def get_patches(self, send_command, download_command=None):
        patches = {
            "DOWNLOAD_WAITING_TIMEOUT": 0,
        }
        if download_command is not None:
            patches["download_data"] = download_command
        return patches

    def test_no_transfer(self):
        self.communicator_mock.send_command = MagicMock()
        run_async(transfer.transfer_data(self.communicator_mock, []))
        self.communicator_mock.send_command.assert_not_called()

    def test_transfer_download(self):
        self.missing_data.copy()
        commands = [
            1,
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_STARTED),
        ]
        send_command = MagicMock(side_effect=partial(send, commands))
        download_command = MagicMock(return_value=coroutine(True))
        result = self._test_workflow(
            send_command, download_command, commands, True, self.missing_data.copy()
        )
        self.assertTrue(result)
        download_command.assert_called_once_with(
            self.missing_data[0], self.communicator_mock
        )

    def test_transfer_wait_download(self):
        commands = [
            1,
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_IN_PROGRESS),
            (self.DOWNLOAD_STARTED_NO_LOCK, self.DOWNLOAD_STARTED),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_STARTED),
        ]
        download_command = MagicMock(return_value=coroutine(True))
        send_command = MagicMock(side_effect=partial(send, commands))
        result = self._test_workflow(
            send_command, download_command, commands, True, self.missing_data.copy()
        )
        self.assertTrue(result)
        download_command.assert_called_once_with(
            self.missing_data[0], self.communicator_mock
        )

    def test_transfer_wait_download_long(self):
        commands = [
            1,
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
        result = self._test_workflow(
            send_command, download_command, commands, True, self.missing_data.copy()
        )
        self.assertTrue(result)
        download_command.assert_called_once_with(
            self.missing_data[0], self.communicator_mock
        )

    def test_transfer_failed(self):
        commands = [
            1,
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_STARTED),
        ]
        download_command = MagicMock(return_value=coroutine(False))
        send_command = MagicMock(side_effect=partial(send, commands))
        result = self._test_workflow(
            send_command, download_command, commands, True, self.missing_data.copy()
        )
        self.assertFalse(result)
        download_command.assert_called_once_with(
            self.missing_data[0], self.communicator_mock
        )

    def test_transfer_downloaded(self):
        commands = [
            1,
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_FINISHED),
        ]
        download_command = MagicMock(return_value=coroutine(True))
        send_command = MagicMock(side_effect=partial(send, commands))
        result = self._test_workflow(
            send_command, download_command, commands, True, self.missing_data.copy()
        )
        self.assertTrue(result)
        download_command.assert_not_called()

    def test_transfer_wait_downloaded(self):
        commands = [
            1,
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_IN_PROGRESS),
            (self.DOWNLOAD_STARTED_NO_LOCK, self.DOWNLOAD_FINISHED),
        ]
        download_command = MagicMock(return_value=coroutine(True))
        send_command = MagicMock(side_effect=partial(send, commands))
        result = self._test_workflow(
            send_command, download_command, commands, True, self.missing_data.copy()
        )
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
                "to_connector": "local",
            },
            {
                "connector_name": "CONNECTOR",
                "url": "URL",
                "data_id": "1",
                "from_storage_location_id": 1,
                "to_storage_location_id": 2,
                "to_connector": "local",
            },
        ]
        commands = [
            1,
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_STARTED),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_STARTED),
        ]
        download_command = MagicMock(side_effect=lambda a, b: coroutine(True))
        send_command = MagicMock(side_effect=partial(send, commands))
        result = self._test_workflow(
            send_command, download_command, commands, True, self.missing_data.copy()
        )
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
                "to_connector": "local",
            },
            {
                "connector_name": "CONNECTOR",
                "url": "URL",
                "data_id": "1",
                "from_storage_location_id": 1,
                "to_storage_location_id": 2,
                "to_connector": "local",
            },
        ]
        commands = [
            1,
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_IN_PROGRESS),
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_STARTED),
            (self.DOWNLOAD_STARTED_NO_LOCK, self.DOWNLOAD_FINISHED),
        ]
        download_command = MagicMock(side_effect=lambda a, b: coroutine(True))
        send_command = MagicMock(side_effect=partial(send, commands))
        result = self._test_workflow(
            send_command, download_command, commands, True, self.missing_data.copy()
        )
        self.assertTrue(result)
        download_command.assert_called_once()

    def test_transfer_protocol_fail(self):
        async def raise_exception(*args, **kwargs):
            raise RuntimeError("Protocol error")

        commands = [
            1,
            (self.DOWNLOAD_STARTED_LOCK, self.DOWNLOAD_STARTED),
        ]
        download_command = MagicMock(return_value=coroutine(True))
        send_command = MagicMock(side_effect=raise_exception)
        with self.assertRaises(RuntimeError):
            self._test_workflow(
                send_command, download_command, commands, True, self.missing_data.copy()
            )
        download_command.assert_not_called()


@patch.dict(sys.modules, MODULES_PATCH)
class DownloadDataTest(BasicTestCase):
    def setUp(self):
        self.communicator_mock = MagicMock()
        self.missing_data = {
            "from_connector": "S3",
            "url": "transfer_url",
            "data_id": 1,
            "from_storage_location_id": 1,
            "to_storage_location_id": 2,
            "to_connector": "local",
        }
        self.COMMAND_DOWNLOAD_FINISHED = Message.command(
            ExecutorProtocol.DOWNLOAD_FINISHED, 2
        )
        self.COMMAND_DOWNLOAD_ABORTED = Message.command(
            ExecutorProtocol.DOWNLOAD_ABORTED, 2
        )
        self.COMMAND_GET_FILES = Message.command(
            ExecutorProtocol.GET_FILES_TO_DOWNLOAD, 1
        )
        self.FILES_LIST = Response(ResponseStatus.OK.value, ["1", "dir/1"])

        return super().setUp()

    def get_patches(self, send_command, transfer_module):
        patches = {
            "Transfer": transfer_module,
            "DataTransferError": DataTransferError,
            "RETRIES": 2,
        }
        return patches

    def _test_workflow_calls(self, send_command, commands):
        self.assertEqual(send_command.call_count, len(commands) - 1)
        expected_args_list = []
        for i in range(1, len(commands)):
            expected_args_list.append(call(commands[i][0]))
        self.assertEqual(expected_args_list, send_command.call_args_list)

    def _test_workflow(
        self, send_command, transfer_module, commands, expected_result=True
    ):
        self.communicator_mock.send_command = send_command
        with patch.multiple(
            TRANSFER,
            **self.get_patches(send_command, transfer_module),
        ):
            result = run_async(
                transfer.download_data(self.missing_data.copy(), self.communicator_mock)
            )
        self.assertEqual(result, expected_result)
        self._test_workflow_calls(send_command, commands)

    def test_download_simple(self):
        commands = [
            1,
            (self.COMMAND_GET_FILES, self.FILES_LIST),
            (self.COMMAND_DOWNLOAD_FINISHED, self.RESULT_OK),
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
            (self.COMMAND_GET_FILES, self.FILES_LIST),
            (self.COMMAND_DOWNLOAD_FINISHED, raise_datatransfererror),
            (self.COMMAND_DOWNLOAD_FINISHED, self.RESULT_OK),
        ]
        send_command = MagicMock(side_effect=partial(send, commands))

        # Mock the transfer object
        transfer_objects = MagicMock()
        transfer = MagicMock(transfer_objects=transfer_objects)
        transfer_module = MagicMock(return_value=transfer)
        self._test_workflow(send_command, transfer_module, commands)
        self.assertEqual(
            transfer_objects.call_args_list,
            [call("transfer_url", ["1", "dir/1"])] * 2,
        )

    def test_download_retry_fail(self):
        commands = [
            1,
            (self.COMMAND_GET_FILES, self.FILES_LIST),
            (self.COMMAND_DOWNLOAD_FINISHED, raise_datatransfererror),
            (self.COMMAND_DOWNLOAD_FINISHED, raise_datatransfererror),
            (self.COMMAND_DOWNLOAD_ABORTED, self.RESULT_OK),
        ]
        send_command = MagicMock(side_effect=partial(send, commands))

        # Mock the transfer object
        transfer_objects = MagicMock()
        transfer = MagicMock(transfer_objects=transfer_objects)
        transfer_module = MagicMock(return_value=transfer)

        self._test_workflow(send_command, transfer_module, commands, False)
        self.assertEqual(
            transfer_objects.call_args_list,
            [call("transfer_url", ["1", "dir/1"])] * 2,
        )
