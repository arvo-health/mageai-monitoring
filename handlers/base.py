"""Base handler class and custom exceptions."""

from abc import ABC, abstractmethod


class HandlerBadRequestError(Exception):
    """Exception raised by handlers to signal a 400 Bad Request error."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


class Handler(ABC):
    """Base class for all event handlers."""

    @abstractmethod
    def match(self, decoded_message: dict) -> bool:
        """
        Determine if this handler should process the event.

        Args:
            decoded_message: The decoded message dictionary to check

        Returns:
            True if this handler should process the event, False otherwise
        """
        pass

    @abstractmethod
    def handle(self, decoded_message: dict) -> None:
        """
        Process the event.

        Args:
            decoded_message: The decoded message dictionary

        Raises:
            HandlerBadRequestError: For 400 errors with a specific message
            Exception: Any other exception will result in a 500 response
        """
        pass
