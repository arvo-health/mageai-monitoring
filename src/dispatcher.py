"""Handler dispatcher for routing events to appropriate handlers."""

import logging
from typing import List
from cloudevents.http import CloudEvent
from flask import Response, make_response

from src.handlers.base import Handler, HandlerBadRequestError


class HandlerDispatcher:
    """Registers and dispatches events to handlers."""
    
    def __init__(self, handlers: List[Handler]):
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
        # Find all matching handlers (exceptions from match() will propagate)
        matched_handlers = []
        for handler in self.handlers:
            try:
                if handler.match(cloud_event):
                    matched_handlers.append(handler)
            except Exception as e:
                # Exception during matching results in 500
                logging.error(
                    f"Handler {handler.__class__.__name__} match() failed: {e}",
                    exc_info=True
                )
                return make_response(("Internal Server Error", 500))
        
        if not matched_handlers:
            logging.warning("No handlers matched the event")
            # Return 204 even if no handlers matched (successful processing)
            return make_response(("", 204))
        
        # Execute all matching handlers in order
        for handler in matched_handlers:
            try:
                handler.handle(cloud_event)
            except HandlerBadRequestError as e:
                # Handler requested a 400 error
                logging.error(
                    f"Handler {handler.__class__.__name__} raised BadRequestError: {e.message}"
                )
                return make_response((e.message, 400))
            except Exception as e:
                # Any other exception results in 500
                logging.error(
                    f"Handler {handler.__class__.__name__} failed: {e}",
                    exc_info=True
                )
                return make_response(("Internal Server Error", 500))
        
        # All handlers succeeded
        return make_response(("", 204))

