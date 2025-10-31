"""Integration tests for BigQuery interactions."""

import pytest
from google.cloud import bigquery


@pytest.mark.integration
def test_bigquery_connection(bigquery_client):
    """Test that we can connect to BigQuery emulator."""
    assert bigquery_client is not None
    assert bigquery_client.project == "test-project"
    
    # Verify we can access the client's project
    project_id = bigquery_client.project
    assert project_id == "test-project"


@pytest.mark.integration
def test_bigquery_dataset_creation(bigquery_client):
    """Test creating a dataset in the BigQuery emulator."""
    dataset_id = "test_dataset"
    
    # Create dataset
    dataset = bigquery.Dataset(f"{bigquery_client.project}.{dataset_id}")
    dataset.location = "US"
    created_dataset = bigquery_client.create_dataset(dataset, exists_ok=True)
    
    assert created_dataset is not None
    assert created_dataset.dataset_id == dataset_id
    
    # Clean up
    bigquery_client.delete_dataset(
        created_dataset.dataset_id, 
        delete_contents=True, 
        not_found_ok=True
    )


@pytest.mark.integration
def test_bigquery_table_creation(bigquery_client):
    """Test creating a table in the BigQuery emulator."""
    dataset_id = "test_dataset"
    table_id = "test_table"
    
    # Create dataset
    dataset = bigquery.Dataset(f"{bigquery_client.project}.{dataset_id}")
    dataset.location = "US"
    bigquery_client.create_dataset(dataset, exists_ok=True)
    
    try:
        # Define table schema
        schema = [
            bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        ]
        
        # Create table
        table = bigquery.Table(
            f"{bigquery_client.project}.{dataset_id}.{table_id}", 
            schema=schema
        )
        created_table = bigquery_client.create_table(table, exists_ok=True)
        
        assert created_table is not None
        assert created_table.table_id == table_id
        
    finally:
        # Clean up
        bigquery_client.delete_dataset(
            dataset_id, 
            delete_contents=True, 
            not_found_ok=True
        )

