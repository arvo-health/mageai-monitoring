"""Seed BigQuery with post-filtered test data."""

import logging

from google.cloud import bigquery

logger = logging.getLogger(__name__)


def seed_post_filtered_data(client: bigquery.Client, project_id: str) -> None:
    """Create datasets and tables with sample post-filtered data for local testing."""

    # Create tmp_athena dataset for selection pipeline
    dataset_id_selection = "tmp_athena"
    dataset_selection = bigquery.Dataset(f"{project_id}.{dataset_id_selection}")
    dataset_selection.location = "US"

    try:
        logger.info(f"Creating dataset: {dataset_id_selection}")
        client.create_dataset(dataset_selection, exists_ok=True)
        logger.info(f"Dataset {dataset_id_selection} created successfully")
    except Exception as e:
        logger.error(f"Failed to create dataset {dataset_id_selection}: {e}")
        raise

    # Define table schema for savings tables
    schema = [
        bigquery.SchemaField("vl_glosa_arvo", "FLOAT", mode="NULLABLE"),
    ]

    # Table names for selection pipeline
    excluded_table_name_selection = "excluded_savings_athena_20251111_163000"
    savings_table_name_selection = "savings_athena_20251111_163000"

    # Create excluded savings table for selection
    excluded_table_ref_selection = (
        f"{project_id}.{dataset_id_selection}.{excluded_table_name_selection}"
    )
    excluded_table_selection = bigquery.Table(excluded_table_ref_selection, schema=schema)

    try:
        logger.info(f"Creating table: {excluded_table_name_selection}")
        client.create_table(excluded_table_selection, exists_ok=True)

        # Insert sample data: total vl_glosa_arvo = 500.0
        excluded_rows_selection = [
            {"vl_glosa_arvo": 150.0},
            {"vl_glosa_arvo": 200.0},
            {"vl_glosa_arvo": 150.0},
        ]
        errors = client.insert_rows_json(excluded_table_ref_selection, excluded_rows_selection)
        if errors:
            logger.error(f"Errors inserting into {excluded_table_name_selection}: {errors}")
            raise Exception(f"Failed to insert rows: {errors}")
        logger.info(
            f"Inserted {len(excluded_rows_selection)} rows into {excluded_table_name_selection}"
        )
    except Exception as e:
        logger.error(f"Failed to create/seed table {excluded_table_name_selection}: {e}")
        raise

    # Create savings table for selection
    savings_table_ref_selection = (
        f"{project_id}.{dataset_id_selection}.{savings_table_name_selection}"
    )
    savings_table_selection = bigquery.Table(savings_table_ref_selection, schema=schema)

    try:
        logger.info(f"Creating table: {savings_table_name_selection}")
        client.create_table(savings_table_selection, exists_ok=True)

        # Insert sample data: total vl_glosa_arvo = 2000.0
        savings_rows_selection = [
            {"vl_glosa_arvo": 500.0},
            {"vl_glosa_arvo": 750.0},
            {"vl_glosa_arvo": 750.0},
        ]
        errors = client.insert_rows_json(savings_table_ref_selection, savings_rows_selection)
        if errors:
            logger.error(f"Errors inserting into {savings_table_name_selection}: {errors}")
            raise Exception(f"Failed to insert rows: {errors}")
        logger.info(
            f"Inserted {len(savings_rows_selection)} rows into {savings_table_name_selection}"
        )
    except Exception as e:
        logger.error(f"Failed to create/seed table {savings_table_name_selection}: {e}")
        raise

    # Create tmp_cemig dataset for approval pipeline
    dataset_id_approval = "tmp_cemig"
    dataset_approval = bigquery.Dataset(f"{project_id}.{dataset_id_approval}")
    dataset_approval.location = "US"

    try:
        logger.info(f"Creating dataset: {dataset_id_approval}")
        client.create_dataset(dataset_approval, exists_ok=True)
        logger.info(f"Dataset {dataset_id_approval} created successfully")
    except Exception as e:
        logger.error(f"Failed to create dataset {dataset_id_approval}: {e}")
        raise

    # Table names for approval pipeline
    excluded_table_name_approval = "excluded_savings_cemig_20251111_100000"
    savings_table_name_approval = "savings_cemig_20251111_100000"

    # Create excluded savings table for approval
    excluded_table_ref_approval = (
        f"{project_id}.{dataset_id_approval}.{excluded_table_name_approval}"
    )
    excluded_table_approval = bigquery.Table(excluded_table_ref_approval, schema=schema)

    try:
        logger.info(f"Creating table: {excluded_table_name_approval}")
        client.create_table(excluded_table_approval, exists_ok=True)

        # Insert sample data: total vl_glosa_arvo = 500.0
        excluded_rows_approval = [
            {"vl_glosa_arvo": 150.0},
            {"vl_glosa_arvo": 200.0},
            {"vl_glosa_arvo": 150.0},
        ]
        errors = client.insert_rows_json(excluded_table_ref_approval, excluded_rows_approval)
        if errors:
            logger.error(f"Errors inserting into {excluded_table_name_approval}: {errors}")
            raise Exception(f"Failed to insert rows: {errors}")
        logger.info(
            f"Inserted {len(excluded_rows_approval)} rows into {excluded_table_name_approval}"
        )
    except Exception as e:
        logger.error(f"Failed to create/seed table {excluded_table_name_approval}: {e}")
        raise

    # Create savings table for approval
    savings_table_ref_approval = f"{project_id}.{dataset_id_approval}.{savings_table_name_approval}"
    savings_table_approval = bigquery.Table(savings_table_ref_approval, schema=schema)

    try:
        logger.info(f"Creating table: {savings_table_name_approval}")
        client.create_table(savings_table_approval, exists_ok=True)

        # Insert sample data: total vl_glosa_arvo = 2000.0
        savings_rows_approval = [
            {"vl_glosa_arvo": 500.0},
            {"vl_glosa_arvo": 750.0},
            {"vl_glosa_arvo": 750.0},
        ]
        errors = client.insert_rows_json(savings_table_ref_approval, savings_rows_approval)
        if errors:
            logger.error(f"Errors inserting into {savings_table_name_approval}: {errors}")
            raise Exception(f"Failed to insert rows: {errors}")
        logger.info(
            f"Inserted {len(savings_rows_approval)} rows into {savings_table_name_approval}"
        )
    except Exception as e:
        logger.error(f"Failed to create/seed table {savings_table_name_approval}: {e}")
        raise

    logger.info("Post-filtered BigQuery seeding completed successfully")
    logger.info(f"Selection dataset: {project_id}.{dataset_id_selection}")
    logger.info(
        f"Selection tables: {excluded_table_name_selection}, {savings_table_name_selection}"
    )
    logger.info(f"Approval dataset: {project_id}.{dataset_id_approval}")
    logger.info(f"Approval tables: {excluded_table_name_approval}, {savings_table_name_approval}")
