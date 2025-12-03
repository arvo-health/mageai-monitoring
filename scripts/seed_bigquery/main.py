"""Seed BigQuery emulator with test data for local development.

This script creates datasets and tables with sample data that match the
example payloads, enabling immediate testing of the service without manual
BigQuery setup.
"""

import logging
import os
import sys
import time
from pathlib import Path

from google.api_core import client_options
from google.cloud import bigquery

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from scripts.seed_bigquery import post_filtered, pre_filtered, processable  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_bigquery_client(project_id: str) -> bigquery.Client:
    """Create a BigQuery client configured for local emulator."""
    emulator_host = os.getenv("BIGQUERY_EMULATOR_HOST", "localhost:9050")
    emulator_endpoint = f"http://{emulator_host}"

    client_options_obj = client_options.ClientOptions(api_endpoint=emulator_endpoint)

    return bigquery.Client(
        project=project_id,
        client_options=client_options_obj,
        credentials=None,  # Emulator doesn't need credentials
    )


def wait_for_emulator(client: bigquery.Client, max_retries: int = 30, delay: float = 1.0) -> bool:
    """Wait for BigQuery emulator to be ready."""
    logger.info("Waiting for BigQuery emulator to be ready...")
    for i in range(max_retries):
        try:
            # Try to list datasets as a health check
            list(client.list_datasets())
            logger.info("BigQuery emulator is ready")
            return True
        except Exception as e:
            if i < max_retries - 1:
                logger.debug(f"Emulator not ready yet (attempt {i + 1}/{max_retries}): {e}")
                time.sleep(delay)
            else:
                logger.error(f"BigQuery emulator failed to become ready: {e}")
                return False
    return False


def main():
    """Main entry point for seeding script."""
    project_id = os.getenv("BIGQUERY_PROJECT_ID", "test-project")
    emulator_host = os.getenv("BIGQUERY_EMULATOR_HOST", "localhost:9050")

    logger.info(f"Seeding BigQuery emulator at {emulator_host}")
    logger.info(f"Using project: {project_id}")

    client = create_bigquery_client(project_id)

    if not wait_for_emulator(client):
        logger.error("BigQuery emulator is not ready. Exiting.")
        sys.exit(1)

    try:
        # Seed pre-filtered, post-filtered, and processable data
        pre_filtered.seed_pre_filtered_data(client, project_id)
        post_filtered.seed_post_filtered_data(client, project_id)
        processable.seed_processable_data(client, project_id)
        logger.info("Seeding completed successfully")
    except Exception as e:
        logger.error(f"Seeding failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
