"""Common utilities for BigQuery seeding scripts."""

import logging
from dataclasses import dataclass

from google.cloud import bigquery

logger = logging.getLogger(__name__)


@dataclass
class TableConfig:
    """Configuration for a table to be created and seeded."""

    name: str
    rows: list[dict]


@dataclass
class DatasetConfig:
    """Configuration for a dataset and its tables."""

    dataset_id: str
    tables: list[TableConfig]


def create_dataset(
    client: bigquery.Client, project_id: str, dataset_id: str, location: str = "US"
) -> None:
    """Create a BigQuery dataset if it doesn't exist."""
    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset.location = location

    try:
        logger.info(f"Creating dataset: {dataset_id}")
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Dataset {dataset_id} created successfully")
    except Exception as e:
        logger.error(f"Failed to create dataset {dataset_id}: {e}")
        raise


def create_and_seed_table(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_config: TableConfig,
    schema: list[bigquery.SchemaField],
) -> None:
    """Create a table and insert sample data."""
    table_ref = f"{project_id}.{dataset_id}.{table_config.name}"
    table = bigquery.Table(table_ref, schema=schema)

    try:
        logger.info(f"Creating table: {table_config.name}")
        client.create_table(table, exists_ok=True)

        errors = client.insert_rows_json(table_ref, table_config.rows)
        if errors:
            logger.error(f"Errors inserting into {table_config.name}: {errors}")
            raise Exception(f"Failed to insert rows: {errors}")
        logger.info(f"Inserted {len(table_config.rows)} rows into {table_config.name}")
    except Exception as e:
        logger.error(f"Failed to create/seed table {table_config.name}: {e}")
        raise
