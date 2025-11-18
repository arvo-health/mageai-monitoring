"""Seed BigQuery with pre-filtered test data."""

import logging

from google.cloud import bigquery

from scripts.seed_bigquery.common import (
    DatasetConfig,
    TableConfig,
    create_and_seed_table,
    create_dataset,
)

logger = logging.getLogger(__name__)


def seed_pre_filtered_data(client: bigquery.Client, project_id: str) -> None:
    """Create datasets and tables with sample pre-filtered data for local testing."""
    schema = [
        bigquery.SchemaField("vl_pago", "FLOAT", mode="NULLABLE"),
    ]

    dataset_config = DatasetConfig(
        dataset_id="tmp_abertta",
        tables=[
            TableConfig(
                name="unprocessable_claims_abertta_20251105_171427",
                rows=[
                    {"vl_pago": 500.0},
                    {"vl_pago": 600.0},
                    {"vl_pago": 400.0},
                ],
            ),
            TableConfig(
                name="processable_claims_abertta_20251105_171427",
                rows=[
                    {"vl_pago": 2000.0},
                    {"vl_pago": 3000.0},
                    {"vl_pago": 3000.0},
                ],
            ),
        ],
    )

    create_dataset(client, project_id, dataset_config.dataset_id)
    for table_config in dataset_config.tables:
        create_and_seed_table(client, project_id, dataset_config.dataset_id, table_config, schema)

    logger.info("Pre-filtered BigQuery seeding completed successfully")
    table_names = ", ".join(table.name for table in dataset_config.tables)
    logger.info(f"Dataset: {project_id}.{dataset_config.dataset_id}")
    logger.info(f"Tables: {table_names}")
