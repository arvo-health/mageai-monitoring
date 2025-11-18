"""BigQuery client factory."""

import os

from google.api_core import client_options
from google.cloud import bigquery

from config import Config


def _create_local_client(project_id: str) -> bigquery.Client:
    """Create a BigQuery client configured for local emulator."""
    emulator_host = os.getenv("BIGQUERY_EMULATOR_HOST", "localhost:9050")
    emulator_endpoint = f"http://{emulator_host}"

    client_options_obj = client_options.ClientOptions(api_endpoint=emulator_endpoint)

    return bigquery.Client(
        project=project_id,
        client_options=client_options_obj,
        credentials=None,  # Emulator doesn't need credentials
    )


def _create_cloud_client(project_id: str) -> bigquery.Client:
    """Create a BigQuery client configured for GCP."""
    return bigquery.Client(project=project_id)


def create_bigquery_client(config: Config) -> bigquery.Client:
    """
    Create a BigQuery client appropriate for the current mode.

    Args:
        config: Application configuration

    Returns:
        Configured BigQuery client instance
    """
    if config.local_mode:
        return _create_local_client(config.bigquery_project_id)
    return _create_cloud_client(config.bigquery_project_id)


__all__ = ["create_bigquery_client"]
