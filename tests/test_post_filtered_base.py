"""Integration tests for PostFilteredBaseHandler."""

import pytest
from cloudevents.http import CloudEvent
from google.cloud import bigquery
from pytest_mock import MockerFixture

import main
from tests.metrics import MetricMatcher


@pytest.mark.integration
def test_post_filtered_base_handler_with_selection_pipeline(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
):
    """Integration test for PostFilteredBaseHandler via selection pipeline.

    This test:
    1. Creates BigQuery tables with test data
    2. Triggers the handler with a pipesv2_selection completion event
    3. Verifies both total and relative metrics are emitted correctly
    """
    dataset_id = "test_dataset"
    excluded_table_id = "excluded_savings"
    savings_table_id = "savings"

    # Create dataset
    dataset = bigquery.Dataset(f"{bigquery_client.project}.{dataset_id}")
    dataset.location = "US"
    bigquery_client.create_dataset(dataset, exists_ok=True)

    try:
        # Define table schema for savings tables
        schema = [
            bigquery.SchemaField("vl_glosa_arvo", "FLOAT", mode="NULLABLE"),
        ]

        # Create excluded savings table and insert test data
        excluded_table = bigquery.Table(
            f"{bigquery_client.project}.{dataset_id}.{excluded_table_id}", schema=schema
        )
        bigquery_client.create_table(excluded_table, exists_ok=True)

        # Insert test data: total vl_glosa_arvo = 500.0
        excluded_rows = [
            {"vl_glosa_arvo": 150.0},
            {"vl_glosa_arvo": 200.0},
            {"vl_glosa_arvo": 150.0},
        ]
        bigquery_client.insert_rows_json(
            f"{bigquery_client.project}.{dataset_id}.{excluded_table_id}", excluded_rows
        )

        # Create savings table and insert test data
        savings_table = bigquery.Table(
            f"{bigquery_client.project}.{dataset_id}.{savings_table_id}", schema=schema
        )
        bigquery_client.create_table(savings_table, exists_ok=True)

        # Insert test data: total vl_glosa_arvo = 2000.0
        savings_rows = [
            {"vl_glosa_arvo": 500.0},
            {"vl_glosa_arvo": 750.0},
            {"vl_glosa_arvo": 750.0},
        ]
        bigquery_client.insert_rows_json(
            f"{bigquery_client.project}.{dataset_id}.{savings_table_id}", savings_rows
        )

        # Create CloudEvent payload for pipesv2_selection pipeline completion
        event = CloudEvent(
            {
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "//pubsub.googleapis.com/projects/test-project/topics/test-topic",
                "specversion": "1.0",
                "id": "test-event-id",
            },
            {
                "source_timestamp": "2024-01-15T10:30:00Z",
                "payload": {
                    "pipeline_uuid": "pipesv2_selection",
                    "status": "COMPLETED",
                    "variables": {
                        "partner": "athena",
                        "excluded_savings_output_table": (
                            f"{bigquery_client.project}.{dataset_id}.{excluded_table_id}"
                        ),
                        "savings_input_table": (
                            f"{bigquery_client.project}.{dataset_id}.{savings_table_id}"
                        ),
                    },
                },
            },
        )

        # Arrange: Declare expected metric emissions
        expected_project = "projects/arvo-eng-prd"
        expected_labels = {"partner": "athena", "approved": "false"}

        expected_calls = [
            # First metric: total value (150 + 200 + 150 = 500.0)
            mocker.call(
                name=expected_project,
                time_series=MetricMatcher(
                    metric_type="claims/pipeline/filtered_post/vl_glosa_arvo/total",
                    value=500.0,
                    labels=expected_labels,
                ),
            ),
            # Second metric: relative value (500.0 / 2000.0 = 0.25)
            mocker.call(
                name=expected_project,
                time_series=MetricMatcher(
                    metric_type="claims/pipeline/filtered_post/vl_glosa_arvo/relative",
                    value=0.25,
                    labels=expected_labels,
                ),
            ),
        ]

        # Act
        response = main.handle_cloud_event(event)

        # Assert: Handler succeeded
        assert response is not None
        status_code = response[1] if isinstance(response, tuple) else response.status_code
        assert status_code == 204

        # Assert: Expected metric calls were made
        # Check that our expected calls are in the call list
        # (may have extra calls from PipelineRunHandler)
        actual_calls = mock_monitoring_client.create_time_series.call_args_list

        # Verify each expected call exists in the actual calls
        for expected_call in expected_calls:
            expected_name = expected_call.kwargs["name"]
            expected_time_series_matcher = expected_call.kwargs["time_series"]

            # Find matching call
            found = False
            for actual_call in actual_calls:
                if actual_call.kwargs.get("name") == expected_name:
                    actual_time_series = actual_call.kwargs.get("time_series", [])
                    if expected_time_series_matcher == actual_time_series:
                        found = True
                        break

            assert found, (
                f"Expected call not found: name={expected_name}, "
                f"time_series={expected_time_series_matcher}"
            )

    finally:
        # Clean up
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_post_filtered_base_handler_with_approval_pipeline(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
):
    """Integration test for PostFilteredBaseHandler via approval pipeline.

    This test:
    1. Creates BigQuery tables with test data
    2. Triggers the handler with a pipesv2_approval completion event
    3. Verifies both total and relative metrics are emitted correctly
    """
    dataset_id = "test_dataset"
    excluded_table_id = "excluded_savings"
    savings_table_id = "savings"

    # Create dataset
    dataset = bigquery.Dataset(f"{bigquery_client.project}.{dataset_id}")
    dataset.location = "US"
    bigquery_client.create_dataset(dataset, exists_ok=True)

    try:
        # Define table schema for savings tables
        schema = [
            bigquery.SchemaField("vl_glosa_arvo", "FLOAT", mode="NULLABLE"),
        ]

        # Create excluded savings table and insert test data
        excluded_table = bigquery.Table(
            f"{bigquery_client.project}.{dataset_id}.{excluded_table_id}", schema=schema
        )
        bigquery_client.create_table(excluded_table, exists_ok=True)

        # Insert test data: total vl_glosa_arvo = 500.0
        excluded_rows = [
            {"vl_glosa_arvo": 150.0},
            {"vl_glosa_arvo": 200.0},
            {"vl_glosa_arvo": 150.0},
        ]
        bigquery_client.insert_rows_json(
            f"{bigquery_client.project}.{dataset_id}.{excluded_table_id}", excluded_rows
        )

        # Create savings table and insert test data
        savings_table = bigquery.Table(
            f"{bigquery_client.project}.{dataset_id}.{savings_table_id}", schema=schema
        )
        bigquery_client.create_table(savings_table, exists_ok=True)

        # Insert test data: total vl_glosa_arvo = 2000.0
        savings_rows = [
            {"vl_glosa_arvo": 500.0},
            {"vl_glosa_arvo": 750.0},
            {"vl_glosa_arvo": 750.0},
        ]
        bigquery_client.insert_rows_json(
            f"{bigquery_client.project}.{dataset_id}.{savings_table_id}", savings_rows
        )

        # Create dummy claims tables for pre-filtered handler
        # (required but not queried by this test)
        unprocessable_table_id = "unprocessable_claims"
        processable_table_id = "processable_claims"
        claims_schema = [
            bigquery.SchemaField("vl_pago", "FLOAT", mode="NULLABLE"),
        ]

        unprocessable_table = bigquery.Table(
            f"{bigquery_client.project}.{dataset_id}.{unprocessable_table_id}", schema=claims_schema
        )
        bigquery_client.create_table(unprocessable_table, exists_ok=True)

        processable_table = bigquery.Table(
            f"{bigquery_client.project}.{dataset_id}.{processable_table_id}", schema=claims_schema
        )
        bigquery_client.create_table(processable_table, exists_ok=True)

        # Create CloudEvent payload for pipesv2_approval pipeline completion
        event = CloudEvent(
            {
                "type": "google.cloud.pubsub.topic.v1.messagePublished",
                "source": "//pubsub.googleapis.com/projects/test-project/topics/test-topic",
                "specversion": "1.0",
                "id": "test-event-id",
            },
            {
                "source_timestamp": "2024-01-15T10:30:00Z",
                "payload": {
                    "pipeline_uuid": "pipesv2_approval",
                    "status": "COMPLETED",
                    "variables": {
                        "partner": "cemig",
                        "unprocessable_claims_input_table": (
                            f"{bigquery_client.project}.{dataset_id}.{unprocessable_table_id}"
                        ),
                        "processable_claims_input_table": (
                            f"{bigquery_client.project}.{dataset_id}.{processable_table_id}"
                        ),
                        "excluded_savings_input_table": (
                            f"{bigquery_client.project}.{dataset_id}.{excluded_table_id}"
                        ),
                        "savings_input_table": (
                            f"{bigquery_client.project}.{dataset_id}.{savings_table_id}"
                        ),
                    },
                },
            },
        )

        # Arrange: Declare expected metric emissions
        expected_project = "projects/arvo-eng-prd"
        expected_labels = {"partner": "cemig", "approved": "true"}

        expected_calls = [
            # First metric: total value (150 + 200 + 150 = 500.0)
            mocker.call(
                name=expected_project,
                time_series=MetricMatcher(
                    metric_type="claims/pipeline/filtered_post/vl_glosa_arvo/total",
                    value=500.0,
                    labels=expected_labels,
                ),
            ),
            # Second metric: relative value (500.0 / 2000.0 = 0.25)
            mocker.call(
                name=expected_project,
                time_series=MetricMatcher(
                    metric_type="claims/pipeline/filtered_post/vl_glosa_arvo/relative",
                    value=0.25,
                    labels=expected_labels,
                ),
            ),
        ]

        # Act
        response = main.handle_cloud_event(event)

        # Assert: Handler succeeded
        assert response is not None
        status_code = response[1] if isinstance(response, tuple) else response.status_code
        assert status_code == 204

        # Assert: Expected metric calls were made
        # Check that our expected calls are in the call list
        # (may have extra calls from PipelineRunHandler)
        actual_calls = mock_monitoring_client.create_time_series.call_args_list

        # Verify each expected call exists in the actual calls
        for expected_call in expected_calls:
            expected_name = expected_call.kwargs["name"]
            expected_time_series_matcher = expected_call.kwargs["time_series"]

            # Find matching call
            found = False
            for actual_call in actual_calls:
                if actual_call.kwargs.get("name") == expected_name:
                    actual_time_series = actual_call.kwargs.get("time_series", [])
                    if expected_time_series_matcher == actual_time_series:
                        found = True
                        break

            assert found, (
                f"Expected call not found: name={expected_name}, "
                f"time_series={expected_time_series_matcher}"
            )

    finally:
        # Clean up
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
