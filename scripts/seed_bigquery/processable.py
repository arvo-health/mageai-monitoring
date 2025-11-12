"""Seed BigQuery with processable test data."""

import logging

from google.cloud import bigquery

from scripts.seed_bigquery.common import (
    DatasetConfig,
    TableConfig,
    create_and_seed_table,
    create_dataset,
)

logger = logging.getLogger(__name__)


def seed_processable_data(client: bigquery.Client, project_id: str) -> None:
    """Create datasets and tables with sample processable data for local testing."""
    schema = [
        bigquery.SchemaField("vl_pago", "FLOAT", mode="NULLABLE"),
    ]

    # Dataset for approval pipeline
    approval_dataset_config = DatasetConfig(
        dataset_id="tmp_cemig",
        tables=[
            TableConfig(
                name="processable_claims_cemig_20251111_100000",
                rows=[
                    {"vl_pago": 2000.0},
                    {"vl_pago": 3000.0},
                    {"vl_pago": 2500.0},
                ],
            ),
            TableConfig(
                name="unprocessable_claims_cemig_20251111_100000",
                rows=[
                    {"vl_pago": 500.0},
                    {"vl_pago": 600.0},
                    {"vl_pago": 400.0},
                ],
            ),
        ],
    )

    # Dataset for wrangling pipeline
    wrangling_dataset_config = DatasetConfig(
        dataset_id="tmp_athena",
        tables=[
            TableConfig(
                name="processable_claims_athena_20251111_163000",
                rows=[
                    {"vl_pago": 1500.0},
                    {"vl_pago": 2000.0},
                    {"vl_pago": 1800.0},
                ],
            ),
            TableConfig(
                name="unprocessable_claims_athena_20251111_163000",
                rows=[
                    {"vl_pago": 300.0},
                    {"vl_pago": 400.0},
                    {"vl_pago": 300.0},
                ],
            ),
        ],
    )

    # Create approval dataset and tables
    create_dataset(client, project_id, approval_dataset_config.dataset_id)
    for table_config in approval_dataset_config.tables:
        create_and_seed_table(
            client, project_id, approval_dataset_config.dataset_id, table_config, schema
        )

    # Create wrangling dataset and tables
    create_dataset(client, project_id, wrangling_dataset_config.dataset_id)
    for table_config in wrangling_dataset_config.tables:
        create_and_seed_table(
            client, project_id, wrangling_dataset_config.dataset_id, table_config, schema
        )

    logger.info("Processable BigQuery seeding completed successfully")
    approval_table_names = ", ".join(table.name for table in approval_dataset_config.tables)
    wrangling_table_names = ", ".join(table.name for table in wrangling_dataset_config.tables)
    logger.info(f"Approval dataset: {project_id}.{approval_dataset_config.dataset_id}")
    logger.info(f"Approval tables: {approval_table_names}")
    logger.info(f"Wrangling dataset: {project_id}.{wrangling_dataset_config.dataset_id}")
    logger.info(f"Wrangling tables: {wrangling_table_names}")
