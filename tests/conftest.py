"""Pytest configuration for unit tests."""

from unittest.mock import MagicMock
import pytest
from google.cloud import monitoring_v3


@pytest.fixture(scope="function")
def mock_monitoring_client(monkeypatch):
    """
    Fixture that provides a mocked Monitoring client for unit testing.
    
    Since Google Cloud Monitoring doesn't have an official emulator,
    we use a mock that captures metric emission calls. This allows us to:
    - Verify that metrics are called with correct parameters
    - Test the full flow without hitting production GCP
    - Validate metric structure, labels, and values
    """
    # Import here to avoid import errors at module level
    from google.cloud import monitoring_v3
    
    mock_client = MagicMock(spec=monitoring_v3.MetricServiceClient)
    
    # Track all calls to create_time_series for assertions
    mock_client.captured_calls = []
    
    def capture_create_time_series(name, time_series):
        """Capture and store metric calls for later inspection."""
        call_data = {
            "project_name": name,
            "time_series": list(time_series),
        }
        mock_client.captured_calls.append(call_data)
        # Return None (successful call)
        return None
    
    mock_client.create_time_series.side_effect = capture_create_time_series
    
    # Patch the global monitoring_client in main.py
    import main
    monkeypatch.setattr(main, "monitoring_client", mock_client)
    
    yield mock_client
    
    # Reset captured calls after test
    mock_client.captured_calls = []


@pytest.fixture(scope="function")
def flask_app():
    """
    Fixture that provides a Flask application context for testing.
    
    Cloud Run functions using Flask's make_response require an app context.
    """
    from flask import Flask
    
    app = Flask(__name__)
    with app.app_context():
        yield app

