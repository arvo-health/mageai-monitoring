"""Seed BigQuery emulator with test data for local development.

This script creates datasets and tables with sample data that match the
example payload in README.md, enabling immediate testing of the service
without manual BigQuery setup.
"""

import logging
import os
import sys
import time

from google.api_core import client_options
from google.cloud import bigquery

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


def seed_bigquery_data(client: bigquery.Client, project_id: str) -> None:
    """Create datasets and tables with sample data for local testing."""

    # Create tmp_abertta dataset
    dataset_id = "tmp_abertta"
    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset.location = "US"

    try:
        logger.info(f"Creating dataset: {dataset_id}")
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Dataset {dataset_id} created successfully")
    except Exception as e:
        logger.error(f"Failed to create dataset {dataset_id}: {e}")
        raise

    # Define table schema for claims tables
    schema = [
        bigquery.SchemaField("vl_pago", "FLOAT", mode="NULLABLE"),
    ]

    # Table names from README example
    unprocessable_table_name = "unprocessable_claims_abertta_20251105_171427"
    processable_table_name = "processable_claims_abertta_20251105_171427"

    # Create unprocessable claims table
    unprocessable_table_ref = f"{project_id}.{dataset_id}.{unprocessable_table_name}"
    unprocessable_table = bigquery.Table(unprocessable_table_ref, schema=schema)

    try:
        logger.info(f"Creating table: {unprocessable_table_name}")
        client.create_table(unprocessable_table, exists_ok=True)

        # Insert sample data: total vl_pago = 1500.0
        unprocessable_rows = [
            {"vl_pago": 500.0},
            {"vl_pago": 600.0},
            {"vl_pago": 400.0},
        ]
        errors = client.insert_rows_json(unprocessable_table_ref, unprocessable_rows)
        if errors:
            logger.error(f"Errors inserting into {unprocessable_table_name}: {errors}")
            raise Exception(f"Failed to insert rows: {errors}")
        logger.info(f"Inserted {len(unprocessable_rows)} rows into {unprocessable_table_name}")
    except Exception as e:
        logger.error(f"Failed to create/seed table {unprocessable_table_name}: {e}")
        raise

    # Create processable claims table
    processable_table_ref = f"{project_id}.{dataset_id}.{processable_table_name}"
    processable_table = bigquery.Table(processable_table_ref, schema=schema)

    try:
        logger.info(f"Creating table: {processable_table_name}")
        client.create_table(processable_table, exists_ok=True)

        # Insert sample data: total vl_pago = 8000.0
        processable_rows = [
            {"vl_pago": 2000.0},
            {"vl_pago": 3000.0},
            {"vl_pago": 3000.0},
        ]
        errors = client.insert_rows_json(processable_table_ref, processable_rows)
        if errors:
            logger.error(f"Errors inserting into {processable_table_name}: {errors}")
            raise Exception(f"Failed to insert rows: {errors}")
        logger.info(f"Inserted {len(processable_rows)} rows into {processable_table_name}")
    except Exception as e:
        logger.error(f"Failed to create/seed table {processable_table_name}: {e}")
        raise

    logger.info("BigQuery seeding completed successfully")
    logger.info(f"Dataset: {project_id}.{dataset_id}")
    logger.info(f"Tables: {unprocessable_table_name}, {processable_table_name}")


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
        seed_bigquery_data(client, project_id)
        logger.info("Seeding completed successfully")
    except Exception as e:
        logger.error(f"Seeding failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
