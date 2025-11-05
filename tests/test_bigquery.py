"""Integration tests for BigQuery interactions."""

import base64
import json
import pytest
from cloudevents.http import CloudEvent
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import main


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
    dataset_id = "test_dataset_creation"
    
    try:
        # Create dataset
        dataset = bigquery.Dataset(f"{bigquery_client.project}.{dataset_id}")
        dataset.location = "US"
        
        # Create with timeout handling
        created_dataset = bigquery_client.create_dataset(dataset, exists_ok=True, timeout=10)
        
        assert created_dataset is not None
        assert created_dataset.dataset_id == dataset_id
        
    finally:
        # Clean up - use not_found_ok to avoid errors if already deleted
        try:
            bigquery_client.delete_dataset(
                dataset_id,
                delete_contents=True,
                not_found_ok=True,
                timeout=10
            )
        except Exception:
            # Ignore cleanup errors
            pass


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

