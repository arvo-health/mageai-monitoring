"""Seed BigQuery with pre-filtered test data."""

import logging

from google.cloud import bigquery

logger = logging.getLogger(__name__)


def seed_pre_filtered_data(client: bigquery.Client, project_id: str) -> None:
    """Create datasets and tables with sample pre-filtered data for local testing."""

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

    logger.info("Pre-filtered BigQuery seeding completed successfully")
    logger.info(f"Dataset: {project_id}.{dataset_id}")
    logger.info(f"Tables: {unprocessable_table_name}, {processable_table_name}")
