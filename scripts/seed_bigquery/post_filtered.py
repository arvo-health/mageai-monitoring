"""Seed BigQuery with post-filtered test data."""

import logging

from google.cloud import bigquery

from scripts.seed_bigquery.common import (
    DatasetConfig,
    TableConfig,
    create_and_seed_table,
    create_dataset,
)

logger = logging.getLogger(__name__)


def seed_post_filtered_data(client: bigquery.Client, project_id: str) -> None:
    """Create datasets and tables with sample post-filtered data for local testing."""
    schema = [
        bigquery.SchemaField("vl_glosa_arvo", "FLOAT", mode="NULLABLE"),
    ]

    datasets_config = [
        DatasetConfig(
            dataset_id="tmp_athena",
            tables=[
                TableConfig(
                    name="excluded_savings_athena_20251111_163000",
                    rows=[
                        {"vl_glosa_arvo": 150.0},
                        {"vl_glosa_arvo": 200.0},
                        {"vl_glosa_arvo": 150.0},
                    ],
                ),
                TableConfig(
                    name="savings_athena_20251111_163000",
                    rows=[
                        {"vl_glosa_arvo": 500.0},
                        {"vl_glosa_arvo": 750.0},
                        {"vl_glosa_arvo": 750.0},
                    ],
                ),
            ],
        ),
        DatasetConfig(
            dataset_id="tmp_cemig",
            tables=[
                TableConfig(
                    name="excluded_savings_cemig_20251111_100000",
                    rows=[
                        {"vl_glosa_arvo": 150.0},
                        {"vl_glosa_arvo": 200.0},
                        {"vl_glosa_arvo": 150.0},
                    ],
                ),
                TableConfig(
                    name="savings_cemig_20251111_100000",
                    rows=[
                        {"vl_glosa_arvo": 500.0},
                        {"vl_glosa_arvo": 750.0},
                        {"vl_glosa_arvo": 750.0},
                    ],
                ),
            ],
        ),
    ]

    for dataset_config in datasets_config:
        create_dataset(client, project_id, dataset_config.dataset_id)
        for table_config in dataset_config.tables:
            create_and_seed_table(
                client, project_id, dataset_config.dataset_id, table_config, schema
            )

    logger.info("Post-filtered BigQuery seeding completed successfully")
    for dataset_config in datasets_config:
        table_names = ", ".join(table.name for table in dataset_config.tables)
        logger.info(f"Dataset: {project_id}.{dataset_config.dataset_id}")
        logger.info(f"Tables: {table_names}")
