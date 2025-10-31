"""Pytest configuration for integration tests."""

import os
import pytest
from google.cloud import bigquery
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs


@pytest.fixture(scope="session")
def bigquery_emulator():
    """
    Fixture that provides a BigQuery emulator container for integration testing.
    
    Uses the bigquery-emulator Docker image which provides a local BigQuery emulator
    that mimics the BigQuery API for testing purposes.
    
    The emulator exposes:
    - gRPC endpoint on port 9050
    - REST API endpoint on port 9060
    
    Note: On ARM64 (Apple Silicon), the image must be pulled with --platform linux/amd64.
    Run: docker pull --platform linux/amd64 ghcr.io/goccy/bigquery-emulator:latest
    """
    container = (
        DockerContainer("ghcr.io/goccy/bigquery-emulator:latest")
        .with_exposed_ports(9050, 9060)
        .with_env("PORT", "9060")
        .with_env("GRPC_PORT", "9050")
    )
    
    container.start()
    
    # Wait a few seconds for the container to initialize
    # The emulator should be ready quickly after container starts
    import time
    time.sleep(3)
    
    yield container
    
    container.stop()


@pytest.fixture
def bigquery_client(bigquery_emulator):
    """
    Fixture that provides a BigQuery client configured to use the emulator.
    
    The client is configured to connect to the emulator endpoint using
    the BIGQUERY_EMULATOR_HOST environment variable, which is the standard
    way to configure the BigQuery Python client for local testing.
    """
    # Get the emulator host and port from the container
    emulator_host = bigquery_emulator.get_container_host_ip()
    emulator_port = bigquery_emulator.get_exposed_port(9060)
    
    # Set the environment variable that BigQuery client uses for emulator endpoint
    os.environ["BIGQUERY_EMULATOR_HOST"] = f"{emulator_host}:{emulator_port}"
    
    try:
        # Create client with test project
        client = bigquery.Client(
            project="test-project",
            # The client will automatically use BIGQUERY_EMULATOR_HOST if set
        )
        
        yield client
        
    finally:
        # Clean up environment variable after tests
        os.environ.pop("BIGQUERY_EMULATOR_HOST", None)

