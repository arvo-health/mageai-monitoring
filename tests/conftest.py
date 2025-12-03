"""Pytest configuration for integration tests."""

import pytest
from google.cloud import bigquery
from pytest_mock import MockerFixture
from tenacity import retry, stop_after_attempt, wait_exponential
from testcontainers.core.container import DockerContainer


@pytest.fixture(scope="session")
def bigquery_emulator():
    """
    Fixture that provides a BigQuery emulator container for integration testing.

    Uses the bigquery-emulator Docker image which provides a local BigQuery emulator
    that mimics the BigQuery API for testing purposes.

    The emulator exposes:
    - REST API endpoint on port 9050 (used by Python client)
    - gRPC endpoint on port 9060

    Note: On ARM64 (Apple Silicon), the container uses platform emulation (linux/x86_64).
    """

    container = (
        DockerContainer("ghcr.io/goccy/bigquery-emulator:latest", platform="linux/x86_64")
        .with_exposed_ports(9050, 9060)
        .with_env("PORT", "9050")  # REST API port (Python client uses this)
        .with_env("GRPC_PORT", "9060")  # gRPC port
        .with_command("--project test-project")  # Required by the emulator
    )

    container.start()

    # Wait for container to be ready and port mapping to be available
    @retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=0.5, min=0.5, max=10))
    def get_container_ports():
        """Get container ports with retry logic."""
        return container.get_container_host_ip(), container.get_exposed_port(9050)

    emulator_host, emulator_port = get_container_ports()

    yield {
        "container": container,
        "host": emulator_host,
        "port": emulator_port,
    }


@pytest.fixture(scope="function")
def bigquery_client(bigquery_emulator, monkeypatch):
    """
    Fixture that provides a BigQuery client configured to use the emulator.

    The client is configured to connect to the emulator endpoint using
    the BIGQUERY_EMULATOR_HOST environment variable, which is the standard
    way to configure the BigQuery Python client for local testing.

    This fixture also monkeypatches bigquery.Client so that any code that
    creates a new client will get one connected to the emulator.
    """
    # Use cached connection info from session-scoped fixture
    emulator_host = bigquery_emulator["host"]
    emulator_port = bigquery_emulator["port"]

    # Create client with test project
    # Configure client to use emulator endpoint explicitly
    from google.api_core import client_options
    from google.auth.credentials import AnonymousCredentials

    emulator_endpoint = f"http://{emulator_host}:{emulator_port}"
    client_options_obj = client_options.ClientOptions(api_endpoint=emulator_endpoint)

    # Create client with anonymous credentials for emulator (no real auth needed)
    client = bigquery.Client(
        project="test-project",
        client_options=client_options_obj,
        credentials=AnonymousCredentials(),  # Use anonymous credentials for emulator
    )

    # Monkeypatch bigquery.Client to always return this test client
    def mock_client_factory(*args, **kwargs):
        return client

    monkeypatch.setattr(bigquery, "Client", mock_client_factory)

    yield client


@pytest.fixture(scope="function")
def mock_monitoring_client(monkeypatch, mocker: MockerFixture):
    """
    Fixture that provides a mocked Monitoring client for integration testing.

    Since Google Cloud Monitoring doesn't have an official emulator,
    we use a mock to capture metric emission calls. This allows us to:
    - Verify that metrics are called with correct parameters
    - Test the full integration flow without hitting production GCP
    - Validate metric structure, labels, and values

    Best practice: This approach provides confidence that metrics will be
    correctly formatted when deployed, while avoiding the complexity and
    cost of using real GCP projects for integration tests.
    """
    from google.cloud import monitoring_v3

    import main
    import metrics

    mock_client = mocker.MagicMock(spec=monitoring_v3.MetricServiceClient)

    # Set the expected project ID for tests
    monkeypatch.setenv("CLOUD_RUN_PROJECT_ID", "arvo-eng-prd")

    # Monkeypatch both the class and the factory function to return our mock
    monkeypatch.setattr(monitoring_v3, "MetricServiceClient", lambda: mock_client)
    # Ensure create_monitoring_client returns our mock regardless of local_mode
    monkeypatch.setattr(metrics, "create_monitoring_client", lambda config: mock_client)
    # Also patch _create_logging_monitoring_client to return our mock
    monkeypatch.setattr(metrics, "_create_logging_monitoring_client", lambda: mock_client)

    # Reset the global dispatcher in main.py so it gets reinitialized with our mock
    monkeypatch.setattr(main, "_dispatcher", None)

    yield mock_client


@pytest.fixture(scope="function")
def sample_pubsub_payload():
    """
    Fixture that provides a sample Pub/Sub payload for testing.

    This represents a typical Mage.ai pipeline run event.
    """
    return {
        "source_timestamp": "2024-01-15T10:30:00Z",
        "payload": {
            "pipeline_uuid": "test_pipeline_123",
            "status": "COMPLETED",
            "variables": {
                "partner": "test_partner",
                "unprocessable_claims_input_table": "test_dataset.test_table",
            },
        },
    }


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


@pytest.fixture(scope="function")
def sample_cloud_event(sample_pubsub_payload):
    """
    Fixture that provides a sample CloudEvent for testing.

    Simulates the CloudEvent structure from Pub/Sub trigger.
    """
    import base64
    import json

    from cloudevents.http import CloudEvent

    # Encode payload as base64 (as Pub/Sub does)
    encoded_data = base64.b64encode(json.dumps(sample_pubsub_payload).encode("utf-8")).decode(
        "utf-8"
    )

    # Create CloudEvent structure
    attributes = {
        "type": "google.cloud.pubsub.topic.v1.messagePublished",
        "source": "//pubsub.googleapis.com/projects/test-project/topics/test-topic",
        "specversion": "1.0",
        "id": "test-event-id",
    }

    data = {
        "message": {
            "data": encoded_data,
            "messageId": "test-message-id",
            "publishTime": "2024-01-15T10:30:00Z",
        }
    }

    event = CloudEvent(attributes, data)
    return event


def assert_response_success(response) -> None:
    """Assert that the handler response indicates success."""
    assert response is not None
    status_code = response[1] if isinstance(response, tuple) else response.status_code
    assert status_code == 204
