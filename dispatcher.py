"""Handler dispatcher for routing events to appropriate handlers."""

import base64
import json
import logging
from datetime import UTC, datetime

from cloudevents.http import CloudEvent
from flask import Response, make_response
from google.cloud import monitoring_v3

from handlers.base import Handler, HandlerBadRequestError
from metrics import emit_gauge_metric


def decode_message(cloud_event: CloudEvent) -> dict | None:
    """
    Decode the message from a CloudEvent.

    Handles two formats:
    1. Base64-encoded message in cloud_event.data["message"]["data"]
    2. Direct payload in cloud_event.data

    Args:
        cloud_event: The CloudEvent to decode

    Returns:
        Decoded message as a dictionary, or None if decoding fails
    """
    data = cloud_event.data

    # Check for base64-encoded message format (Pub/Sub style)
    message = data.get("message", {})
    encoded_data = message.get("data")

    if encoded_data:
        try:
            decoded = base64.b64decode(encoded_data).decode("utf-8")
            return json.loads(decoded)
        except (ValueError, json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.error(f"Failed to decode base64 message: {e}")
            return None

    # Check for direct payload format (wrangling events)
    if "payload" in data and "source_timestamp" in data:
        return data

    # If neither format matches, return None
    return None


class HandlerDispatcher:
    """Registers and dispatches events to handlers."""

    def __init__(
        self,
        handlers: list[Handler],
        monitoring_client: monitoring_v3.MetricServiceClient,
        project_id: str,
    ):
        """
        Initialize the dispatcher with a list of handlers.

        Args:
            handlers: List of handler instances to register
            monitoring_client: Monitoring client for emitting metrics
            project_id: Project ID for metric emission
        """
        self.handlers = handlers
        self.monitoring_client = monitoring_client
        self.project_id = project_id

    def dispatch(self, cloud_event: CloudEvent) -> Response:
        """
        Dispatch event to all matching handlers.

        Accumulates errors from matchers and handlers, executes all that work,
        and emits metrics for failures before returning a response.

        Args:
            cloud_event: The raw CloudEvent

        Returns:
            HTTP response:
            - 204 NO CONTENT if all handlers succeed
            - 400 if any handler raises HandlerBadRequestError
            - 500 if any handler raises any other exception (including during matching)
        """
        timestamp = datetime.now(UTC)

        # Decode the message from the cloud_event
        decoded_message = decode_message(cloud_event)

        if decoded_message is None:
            logging.error("Failed to decode message from CloudEvent")
            return make_response(("Bad Request: Invalid message format", 400))

        # Log the decoded message
        logging.info(f"Decoded message: {json.dumps(decoded_message, default=str)}")

        # Accumulate errors from matchers
        matcher_errors = []
        matched_handlers = []
        for handler in self.handlers:
            try:
                if handler.match(decoded_message):
                    matched_handlers.append(handler)
            except Exception as e:
                # Accumulate matcher error instead of returning immediately
                handler_class = handler.__class__.__name__
                logging.error(f"Handler {handler_class} match() failed: {e}", exc_info=True)
                matcher_errors.append((handler_class, e))

        # Accumulate errors from handlers
        handler_errors = []
        for handler in matched_handlers:
            try:
                handler.handle(decoded_message)
            except HandlerBadRequestError as e:
                # Accumulate BadRequestError
                handler_class = handler.__class__.__name__
                logging.error(f"Handler {handler_class} raised BadRequestError: {e.message}")
                handler_errors.append((handler_class, e, "bad_request"))
            except Exception as e:
                # Accumulate other exceptions
                handler_class = handler.__class__.__name__
                logging.error(f"Handler {handler_class} failed: {e}", exc_info=True)
                handler_errors.append((handler_class, e, "internal_error"))

        # Emit metrics for all failures
        for handler_class, error in matcher_errors:
            emit_gauge_metric(
                monitoring_client=self.monitoring_client,
                project_id=self.project_id,
                name="handler_matcher_failure",
                value=1.0,
                labels={"handler_class": handler_class},
                timestamp=timestamp,
            )

        for handler_class, error, error_type in handler_errors:
            emit_gauge_metric(
                monitoring_client=self.monitoring_client,
                project_id=self.project_id,
                name="handler_execution_failure",
                value=1.0,
                labels={"handler_class": handler_class},
                timestamp=timestamp,
            )

        # Determine response based on accumulated errors
        # If there's any matcher error, respond with 500
        if matcher_errors:
            return make_response(("Internal Server Error", 500))

        if handler_errors:
            # Check if any handler raised BadRequestError
            bad_request_errors = [
                (handler_class, error)
                for handler_class, error, et in handler_errors
                if et == "bad_request"
            ]
            if bad_request_errors:
                # Return the first bad request error message
                return make_response((bad_request_errors[0][1].message, 400))
            else:
                # All other errors result in 500
                return make_response(("Internal Server Error", 500))

        if not matched_handlers:
            logging.warning("No handlers matched the event")
            # Return 204 even if no handlers matched (successful processing)
            return make_response(("", 204))

        # All handlers succeeded
        return make_response(("", 204))
