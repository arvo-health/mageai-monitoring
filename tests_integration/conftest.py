"""Pytest configuration for integration tests."""

import os
import platform
import pytest
from google.cloud import bigquery
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
    
    # Get connection info once and cache it to avoid repeated get_exposed_port() calls
    # which can hang on subsequent calls
    # Use port 9050 for REST API (which the Python client uses)
    emulator_host = container.get_container_host_ip()
    emulator_port = container.get_exposed_port(9050)
    
    yield {
        "container": container,
        "host": emulator_host,
        "port": emulator_port,
    }
    
    container.stop()


@pytest.fixture(scope="function")
def bigquery_client(bigquery_emulator):
    """
    Fixture that provides a BigQuery client configured to use the emulator.
    
    The client is configured to connect to the emulator endpoint using
    the BIGQUERY_EMULATOR_HOST environment variable, which is the standard
    way to configure the BigQuery Python client for local testing.
    """
    # Use cached connection info from session-scoped fixture
    emulator_host = bigquery_emulator["host"]
    emulator_port = bigquery_emulator["port"]
    
    # Create client with test project
    # Configure client to use emulator endpoint explicitly
    from google.api_core import client_options
    
    emulator_endpoint = f"http://{emulator_host}:{emulator_port}"
    client_options_obj = client_options.ClientOptions(
        api_endpoint=emulator_endpoint
    )
    
    # Create client without credentials to avoid auth delays
    client = bigquery.Client(
        project="test-project",
        client_options=client_options_obj,
        credentials=None,  # Emulator doesn't need real credentials
    )
    
    yield client

