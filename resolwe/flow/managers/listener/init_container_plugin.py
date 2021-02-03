"""Commands specific to kubernetes installation."""

from typing import TYPE_CHECKING

from resolwe.flow.executors.socket_utils import Message, Response
from resolwe.flow.utils import iterate_fields
from resolwe.storage.models import FileStorage, ReferencedPath

from .plugin import ListenerPlugin

if TYPE_CHECKING:
    from resolwe.flow.managers.listener.listener import Processor


class InitContainerPlugin(ListenerPlugin):
    """Handler methods for Init Container."""

    name = "Init container plugin"

    def handle_get_inputs_no_shared_storage(
        self, message: Message[int], manager: "Processor"
    ) -> Response:
        """Get a files belonging to input data objects.

        The format of the output is as follows:
        {
            base_url_1: (connector1, [list, of, ReferencedPath, instances]),
            bose_url_2: (connector2, [another, list, of, ReferencedPath, instances])
            ...
        }
        """
        output_data = {}

        # First get ids of data objecs which are inputs for data object we are
        # processing.
        input_data_ids = []
        for schema, fields in iterate_fields(
            manager.data.input, manager.data.process.input_schema
        ):
            type_ = schema["type"]
            if type_.startswith("data:") or type_.startswith("list:data:"):
                value = fields[schema["name"]]
                if isinstance(value, int):
                    input_data_ids.append(value)
                else:
                    input_data_ids += value

        for input_data_id in input_data_ids:
            file_storage = FileStorage.objects.get(data=input_data_id)
            location = file_storage.default_storage_location
            output_data[location.url] = (
                location.connector_name,
                list(
                    ReferencedPath.objects.filter(storage_locations=location).values()
                ),
            )

        manager._listener.communicator.suspend_heartbeat(manager.peer_identity)
        return message.respond_ok(output_data)
