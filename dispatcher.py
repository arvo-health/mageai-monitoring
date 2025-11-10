"""Handler dispatcher for routing events to appropriate handlers."""

import base64
import json
import logging

from cloudevents.http import CloudEvent
from flask import Response, make_response

from handlers.base import Handler, HandlerBadRequestError


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

    def __init__(self, handlers: list[Handler]):
        """
        Initialize the dispatcher with a list of handlers.

        Args:
            handlers: List of handler instances to register
        """
        self.handlers = handlers

    def dispatch(self, cloud_event: CloudEvent) -> Response:
        """
        Dispatch event to all matching handlers.

        Args:
            cloud_event: The raw CloudEvent

        Returns:
            HTTP response:
            - 204 NO CONTENT if all handlers succeed
            - 400 if any handler raises HandlerBadRequestError
            - 500 if any handler raises any other exception (including during matching)
        """
        # Decode the message from the cloud_event
        decoded_message = decode_message(cloud_event)

        if decoded_message is None:
            logging.error("Failed to decode message from CloudEvent")
            return make_response(("Bad Request: Invalid message format", 400))

        # Log the decoded message
        logging.info(f"Decoded message: {json.dumps(decoded_message, default=str)}")

        # Find all matching handlers (exceptions from match() will propagate)
        matched_handlers = []
        for handler in self.handlers:
            try:
                if handler.match(decoded_message):
                    matched_handlers.append(handler)
            except Exception as e:
                # Exception during matching results in 500
                logging.error(
                    f"Handler {handler.__class__.__name__} match() failed: {e}", exc_info=True
                )
                return make_response(("Internal Server Error", 500))

        if not matched_handlers:
            logging.warning("No handlers matched the event")
            # Return 204 even if no handlers matched (successful processing)
            return make_response(("", 204))

        # Execute all matching handlers in order
        for handler in matched_handlers:
            try:
                handler.handle(decoded_message)
            except HandlerBadRequestError as e:
                # Handler requested a 400 error
                logging.error(
                    f"Handler {handler.__class__.__name__} raised BadRequestError: {e.message}"
                )
                return make_response((e.message, 400))
            except Exception as e:
                # Any other exception results in 500
                logging.error(f"Handler {handler.__class__.__name__} failed: {e}", exc_info=True)
                return make_response(("Internal Server Error", 500))

        # All handlers succeeded
        return make_response(("", 204))
