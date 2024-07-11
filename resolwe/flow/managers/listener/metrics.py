"""Report the listener metrics to the monitoring system."""

import logging
import os
from time import time
from typing import Any

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

from resolwe.flow.executors.socket_utils import (
    Message,
    MessageProcessingCallback,
    MessageProcessingEventType,
)

logger = logging.getLogger(__name__)

# How often to export metrics (in milliseconds).
METRICS_EXPORT_INTERVAL = 60000
# TODO: Remove this when metrics are properly implemented.
METRICS_INSECURE_EXPORT = True


class MetricsEventReporter(MessageProcessingCallback):
    """Class handling the reporting of the metrics events."""

    # The prefix used in all metric names.
    metrics_prefix = "resolwe_listener_"

    def __init__(self):
        """Initialize the metrics event reporter."""
        if (endpoint := os.environ.get("METRICS_ENDPOINT")) is not None:
            self._enabled = False
            logger.warning(
                "Listener metrics endpoint is not set, diabling metrics reporting."
            )
        else:
            self._enabled = True
            self._init_metrics(endpoint)

    def _init_metrics(self, metric_endpoint):
        # Initialize the counters for the metrics.
        self._processing_messages = 0
        self._queued_messages = 0

        # Create the dictionaries with the appropriate timestamps.
        self._processing_started: dict[bytes, float] = {}
        self._message_received: dict[bytes, float] = {}

        # Initialize the opentelemetry metrics.
        metric_exporter = OTLPMetricExporter(
            endpoint=metric_endpoint, insecure=METRICS_INSECURE_EXPORT
        )
        metric_reader = PeriodicExportingMetricReader(
            metric_exporter, METRICS_EXPORT_INTERVAL
        )
        provider = MeterProvider(
            metric_readers=[metric_reader],
            resource=Resource.create({"service.name": "listener"}),
        )

        # Sets the global default meter provider
        metrics.set_meter_provider(provider)

        # Creates a meter from the global meter provider
        meter = metrics.get_meter("listener")

        # Create the metrics.
        self._messages_queued = meter.create_up_down_counter(
            name=f"{self.metrics_prefix}_messages_in_queue",
            description="Number of unprocessed messages in the queue.",
        )
        self._messages_processing = meter.create_up_down_counter(
            name=f"{self.metrics_prefix}_messages_processing",
            description="Number of currently processing messages.",
        )
        self._preparation_time_histogram = meter.create_histogram(
            name=f"{self.metrics_prefix}_preparation_duration",
            description="Response time for messages.",
            unit="seconds",
        )
        self._response_time_histogram = meter.create_histogram(
            name=f"{self.metrics_prefix}_response_time_duration",
            unit="seconds",
            description="Response time for messages.",
        )
        self._processing_time_histogram = meter.create_histogram(
            name=f"{self.metrics_prefix}_processing_time_duration",
            unit="seconds",
            description="Processing time for messages.",
        )
        self._network_time_histogram = meter.create_histogram(
            name=f"{self.metrics_prefix}_network_duration",
            unit="seconds",
            description="Time messages spent in the transfer.",
        )

    def _counter_changed(
        self, counter: metrics.UpDownCounter, value: int, attributes: dict
    ):
        """Change the value of the given counter."""
        counter.add(value, attributes)

    def _attributes_from_message(self, message: Message) -> dict[str, Any]:
        """Create the attributes from the given message."""
        return {"command": message.type_data, "data_id": message.client_id}

    def event(self, event_type: MessageProcessingEventType, message: Message):
        """Process the event from the message processing pipeline."""
        # Do not proceed if metric reporting is disabled.
        if self._enabled is False:
            return

        if message.client_id is None:
            logger.error("Message client_id is not defined.")
            return

        attributes = self._attributes_from_message(message)
        match event_type:
            case MessageProcessingEventType.MESSAGE_RECEIVED:
                self._queued_messages += 1
                self._counter_changed(self._messages_queued, 1, attributes)
                self._message_received[message.client_id] = time()
                self._network_time_histogram.record(
                    self._message_received[message.client_id] - message.sent_timestamp,
                    attributes,
                )

            case MessageProcessingEventType.MESSAGE_PROCESSING_STARTED:
                self._processing_messages += 1
                self._counter_changed(self._messages_processing, 1, attributes)
                self._processing_started[message.client_id] = time()

            case MessageProcessingEventType.PREPARATION_FINISHED:
                if message.client_id not in self._processing_started:
                    logger.error(
                        "Message from client '%s' not found in the processing messages.",
                        message.client_id,
                    )
                    return
                preparation_finished = time()
                preparation_time = preparation_finished - self._processing_started.pop(
                    message.client_id
                )
                self._preparation_time_histogram.record(preparation_time, attributes)

            case MessageProcessingEventType.MESSAGE_PROCESSING_FINISHED:
                self._processing_messages -= 1
                self._queued_messages -= 1
                self._counter_changed(self._messages_processing, -1, attributes)
                self._counter_changed(self._messages_queued, -1, attributes)

                if message.client_id not in self._processing_started:
                    logger.error(
                        "Message from client '%s' not found in the processing messages.",
                        message.client_id,
                    )
                    return
                if message.client_id not in self._message_received:
                    logger.error(
                        "Message from client '%s' not found in the received messages.",
                        message.client_id,
                    )
                    return

                processing_started = self._processing_started.pop(message.client_id)
                message_received = self._message_received.pop(message.client_id)
                processing_finished = time()
                response_time = processing_finished - message_received
                processing_time = processing_finished - processing_started
                self._response_time_histogram.record(response_time, attributes)
                self._processing_time_histogram.record(processing_time, attributes)


metrics_reporter = MetricsEventReporter()
