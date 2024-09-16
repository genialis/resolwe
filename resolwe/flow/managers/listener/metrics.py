"""Report the listener metrics to the monitoring system."""

import logging
import socket
from time import time
from typing import Any

from django.conf import settings
from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

from resolwe.flow.executors.socket_utils import (
    Message,
    MessageProcessingCallback,
    MessageProcessingEventType,
    PeerIdentity,
)

logger = logging.getLogger(__name__)

# How often to export metrics (in milliseconds).
METRICS_EXPORT_INTERVAL = 30000
# TODO: Remove this when metrics are properly implemented.
METRICS_INSECURE_EXPORT = True


class MetricsEventReporter(MessageProcessingCallback):
    """Class handling the reporting of the metrics events."""

    # The prefix used in all metric names. The name is generated as
    # f"{self.metrics_prefix}_{metric_name}", so avoid adding '_' at the end.
    metrics_prefix = "resolwe_listener"

    def __init__(self):
        """Initialize the metrics event reporter."""
        if not (endpoint := getattr(settings, "FLOW_METRICS_ENDPOINT", None)):
            logger.info("Metrics endpoint is not set, reporting disabled.")
            self._enabled = False
            logger.warning(
                "Listener metrics endpoint is not set, diabling metrics reporting."
            )
        else:
            logger.info("Reporting metrics to endpoint %s.", endpoint)
            self._hostname = socket.gethostname()
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
            endpoint=metric_endpoint,
            insecure=METRICS_INSECURE_EXPORT,
            max_export_batch_size=getattr(settings, "FLOW_METRICS_EXPORT_SIZE", 1000),
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
            name=f"{self.metrics_prefix}_messages_queued",
            description="Number of unprocessed messages in the queue.",
        )
        self._messages_processing = meter.create_up_down_counter(
            name=f"{self.metrics_prefix}_messages_processing",
            description="Number of currently processing messages.",
        )
        self._preparation_time_histogram = meter.create_histogram(
            name=f"{self.metrics_prefix}_preparation_duration",
            description="The duration of init phase.",
            unit="seconds",
        )
        self._response_time_histogram = meter.create_histogram(
            name=f"{self.metrics_prefix}_response_duration",
            unit="miliseconds",
            description="Response time for messages.",
        )
        self._processing_time_histogram = meter.create_histogram(
            name=f"{self.metrics_prefix}_processing_duration",
            unit="miliseconds",
            description="Processing time for messages.",
        )
        self._network_time_histogram = meter.create_histogram(
            name=f"{self.metrics_prefix}_network_duration",
            unit="miliseconds",
            description="Time messages spent in the transfer.",
        )

    def _counter_changed(
        self, counter: metrics.UpDownCounter, value: int, attributes: dict
    ):
        """Change the value of the given counter."""
        counter.add(value, attributes)

    def _attributes_from_message(self, message: Message) -> dict[str, Any]:
        """Create the attributes from the given message."""
        return {"command": message.type_data}

    def event(
        self,
        event_type: MessageProcessingEventType,
        message: Message,
        peer_identity: PeerIdentity,
        **kwargs,
    ):
        """Process the event from the message processing pipeline."""
        # Do not proceed if metric reporting is disabled.
        if not self._enabled:
            return

        if message.client_id is None:
            logger.error("Message client_id is not defined.")
            return

        attributes = self._attributes_from_message(message)
        attributes["pod"] = self._hostname

        match event_type:
            case MessageProcessingEventType.MESSAGE_RECEIVED:
                self._queued_messages += 1
                self._counter_changed(self._messages_queued, 1, attributes)
                self._message_received[message.client_id] = time()
                value = (
                    self._message_received[message.client_id] - message.sent_timestamp
                )
                self._network_time_histogram.record(value * 1000, attributes)

            case MessageProcessingEventType.MESSAGE_PROCESSING_STARTED:
                self._processing_messages += 1
                self._counter_changed(self._messages_processing, 1, attributes)
                self._processing_started[message.client_id] = time()

            case MessageProcessingEventType.PREPARATION_FINISHED:
                # This type expects the 'started' unix timestamp in kwargs.
                if "started" not in kwargs:
                    logger.error(
                        "Extra data 'started' expected for preparation finished event."
                    )
                    return
                preparation_time = time() - kwargs["started"]
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
                self._response_time_histogram.record(response_time * 1000, attributes)
                self._processing_time_histogram.record(
                    processing_time * 1000, attributes
                )


metrics_reporter = MetricsEventReporter()
