"""Shared BigQuery test utilities."""

from google.cloud import bigquery


def create_dataset(bigquery_client, dataset_id: str) -> None:
    """Create a BigQuery dataset for testing."""
    dataset = bigquery.Dataset(f"{bigquery_client.project}.{dataset_id}")
    dataset.location = "US"
    bigquery_client.create_dataset(dataset, exists_ok=True)


def create_claims_table_with_data(
    bigquery_client, dataset_id: str, table_id: str, rows: list[dict]
) -> None:
    """Create a claims table and insert test data."""
    schema = [
        bigquery.SchemaField("vl_pago", "FLOAT", mode="NULLABLE"),
    ]
    table = bigquery.Table(f"{bigquery_client.project}.{dataset_id}.{table_id}", schema=schema)
    bigquery_client.create_table(table, exists_ok=True)
    bigquery_client.insert_rows_json(f"{bigquery_client.project}.{dataset_id}.{table_id}", rows)


def create_savings_table_with_data(
    bigquery_client, dataset_id: str, table_id: str, rows: list[dict]
) -> None:
    """Create a savings table and insert test data."""
    schema = [
        bigquery.SchemaField("vl_glosa_arvo", "FLOAT", mode="NULLABLE"),
    ]
    table = bigquery.Table(f"{bigquery_client.project}.{dataset_id}.{table_id}", schema=schema)
    bigquery_client.create_table(table, exists_ok=True)
    bigquery_client.insert_rows_json(f"{bigquery_client.project}.{dataset_id}.{table_id}", rows)


def create_savings_tables(bigquery_client, dataset_id: str) -> tuple[str, str]:
    """Create dummy savings tables for post-filtered handler (required but not queried)."""
    excluded_table_id = "excluded_savings"
    savings_table_id = "savings"
    savings_schema = [
        bigquery.SchemaField("vl_glosa_arvo", "FLOAT", mode="NULLABLE"),
    ]

    for table_id in [excluded_table_id, savings_table_id]:
        table = bigquery.Table(
            f"{bigquery_client.project}.{dataset_id}.{table_id}", schema=savings_schema
        )
        bigquery_client.create_table(table, exists_ok=True)

    return excluded_table_id, savings_table_id


def create_claims_tables(bigquery_client, dataset_id: str) -> tuple[str, str]:
    """Create dummy claims tables for pre-filtered handler (required but not queried)."""
    unprocessable_table_id = "unprocessable_claims"
    processable_table_id = "processable_claims"
    claims_schema = [
        bigquery.SchemaField("vl_pago", "FLOAT", mode="NULLABLE"),
    ]

    for table_id in [unprocessable_table_id, processable_table_id]:
        table = bigquery.Table(
            f"{bigquery_client.project}.{dataset_id}.{table_id}", schema=claims_schema
        )
        bigquery_client.create_table(table, exists_ok=True)

    return unprocessable_table_id, processable_table_id
