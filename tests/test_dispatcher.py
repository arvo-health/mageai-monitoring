"""Tests for HandlerDispatcher."""

from dispatcher import HandlerDispatcher
from handlers.base import Handler


class MockHandler(Handler):
    """Mock handler for testing."""

    def __init__(self, name, should_match=True, should_raise=None):
        """
        Initialize mock handler.

        Args:
            name: Handler name for identification
            should_match: Whether match() should return True
            should_raise: Exception to raise in handle(), or None
        """
        self.name = name
        self.should_match = should_match
        self.should_raise = should_raise

    def match(self, decoded_message: dict) -> bool:
        """Match handler."""
        return self.should_match

    def handle(self, decoded_message: dict) -> None:
        """Handle event."""
        if self.should_raise:
            raise self.should_raise


class FailingMatcherHandler(Handler):
    """Handler that fails during match()."""

    def __init__(self, name):
        self.name = name

    def match(self, decoded_message: dict) -> bool:
        """Raise exception during match."""
        raise ValueError(f"Matcher error in {self.name}")

    def handle(self, decoded_message: dict) -> None:
        """Never called."""
        pass


def test_dispatcher_successful_dispatch(mock_monitoring_client, sample_cloud_event, flask_app):
    """Test successful dispatch when all handlers match and execute successfully."""
    # Arrange
    handler1 = MockHandler("Handler1", should_match=True)
    handler2 = MockHandler("Handler2", should_match=True)
    handlers = [handler1, handler2]

    dispatcher = HandlerDispatcher(
        handlers=handlers,
        monitoring_client=mock_monitoring_client,
        project_id="test-project",
    )

    # Act
    response = dispatcher.dispatch(sample_cloud_event)

    # Assert
    assert response is not None
    status_code = response[1] if isinstance(response, tuple) else response.status_code
    assert status_code == 204

    # Verify no metrics were emitted (no failures)
    mock_monitoring_client.create_time_series.assert_not_called()


def test_dispatcher_matcher_error(mock_monitoring_client, sample_cloud_event, flask_app):
    """Test dispatcher when matcher raises an error - should return 500 and emit metric."""
    # Arrange
    failing_handler = FailingMatcherHandler("FailingHandler")
    working_handler = MockHandler("WorkingHandler", should_match=False)
    handlers = [failing_handler, working_handler]

    dispatcher = HandlerDispatcher(
        handlers=handlers,
        monitoring_client=mock_monitoring_client,
        project_id="test-project",
    )

    # Act
    response = dispatcher.dispatch(sample_cloud_event)

    # Assert
    assert response is not None
    status_code = response[1] if isinstance(response, tuple) else response.status_code
    assert status_code == 500

    # Verify metric was emitted for matcher failure
    assert mock_monitoring_client.create_time_series.called
    call_args = mock_monitoring_client.create_time_series.call_args

    # Check the call was made with correct project name
    assert call_args.kwargs["name"] == "projects/test-project"

    # Check time_series contains the matcher failure metric
    time_series_list = call_args.kwargs["time_series"]
    assert len(time_series_list) == 1
    ts = time_series_list[0]
    assert ts.metric.type == "custom.googleapis.com/handler_matcher_failure"
    assert dict(ts.metric.labels) == {"handler_class": "FailingMatcherHandler"}


def test_dispatcher_handler_error(mock_monitoring_client, sample_cloud_event, flask_app):
    """Test dispatcher when handler raises an error - should return 500 and emit metric."""
    # Arrange
    failing_handler = MockHandler(
        "FailingHandler", should_match=True, should_raise=RuntimeError("Handler failed")
    )
    working_handler = MockHandler("WorkingHandler", should_match=True)
    handlers = [failing_handler, working_handler]

    dispatcher = HandlerDispatcher(
        handlers=handlers,
        monitoring_client=mock_monitoring_client,
        project_id="test-project",
    )

    # Act
    response = dispatcher.dispatch(sample_cloud_event)

    # Assert
    assert response is not None
    status_code = response[1] if isinstance(response, tuple) else response.status_code
    assert status_code == 500

    # Verify metric was emitted for handler execution failure
    assert mock_monitoring_client.create_time_series.called
    call_args = mock_monitoring_client.create_time_series.call_args

    # Check the call was made with correct project name
    assert call_args.kwargs["name"] == "projects/test-project"

    # Check time_series contains the handler failure metric
    time_series_list = call_args.kwargs["time_series"]
    assert len(time_series_list) == 1
    ts = time_series_list[0]
    assert ts.metric.type == "custom.googleapis.com/handler_execution_failure"
    assert dict(ts.metric.labels) == {"handler_class": "MockHandler"}
