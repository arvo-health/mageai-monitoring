"""Integration tests for PreFilteredBaseHandler."""

from unittest.mock import call

import pytest
from cloudevents.http import CloudEvent
from google.cloud import bigquery

import main


class MetricMatcher:
    """Custom matcher for verifying TimeSeries metric properties."""

    def __init__(self, metric_type, value, labels):
        self.metric_type = metric_type
        self.value = value
        self.labels = labels

    def __eq__(self, time_series_list):
        """Match against a list containing a single TimeSeries."""
        if len(time_series_list) != 1:
            return False
        ts = time_series_list[0]
        return (
            ts.metric.type == f"custom.googleapis.com/{self.metric_type}"
            and dict(ts.metric.labels) == self.labels
            and len(ts.points) == 1
            and ts.points[0].value.double_value == self.value
        )

    def __repr__(self):
        return f"MetricMatcher(type={self.metric_type}, value={self.value}, labels={self.labels})"


@pytest.mark.integration
def test_pre_filtered_base_handler_with_approval_pipeline(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
):
    """Integration test for PreFilteredBaseHandler via approval pipeline.

    This test:
    1. Creates BigQuery tables with test data
    2. Triggers the handler with a pipesv2_approval completion event
    3. Verifies both total and relative metrics are emitted correctly
    """
    dataset_id = "test_dataset"
    unprocessable_table_id = "unprocessable_claims"
    processable_table_id = "processable_claims"

    # Create dataset
    dataset = bigquery.Dataset(f"{bigquery_client.project}.{dataset_id}")
    dataset.location = "US"
    bigquery_client.create_dataset(dataset, exists_ok=True)

    try:
        # Define table schema for claims tables
        schema = [
            bigquery.SchemaField("vl_pago", "FLOAT", mode="NULLABLE"),
        ]

        # Create unprocessable claims table and insert test data
        unprocessable_table = bigquery.Table(
            f"{bigquery_client.project}.{dataset_id}.{unprocessable_table_id}", schema=schema
        )
        bigquery_client.create_table(unprocessable_table, exists_ok=True)

        # Insert test data: total vl_pago = 1000.0
        unprocessable_rows = [
            {"vl_pago": 300.0},
            {"vl_pago": 400.0},
            {"vl_pago": 300.0},
        ]
        bigquery_client.insert_rows_json(
            f"{bigquery_client.project}.{dataset_id}.{unprocessable_table_id}", unprocessable_rows
        )

        # Create processable claims table and insert test data
        processable_table = bigquery.Table(
            f"{bigquery_client.project}.{dataset_id}.{processable_table_id}", schema=schema
        )
        bigquery_client.create_table(processable_table, exists_ok=True)

        # Insert test data: total vl_pago = 4000.0
        processable_rows = [
            {"vl_pago": 1000.0},
            {"vl_pago": 1500.0},
            {"vl_pago": 1500.0},
        ]
        bigquery_client.insert_rows_json(
            f"{bigquery_client.project}.{dataset_id}.{processable_table_id}", processable_rows
        )

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
                        "partner": "porto",
                        "unprocessable_claims_input_table": (
                            f"{bigquery_client.project}.{dataset_id}.{unprocessable_table_id}"
                        ),
                        "processable_claims_input_table": (
                            f"{bigquery_client.project}.{dataset_id}.{processable_table_id}"
                        ),
                    },
                },
            },
        )

        # Arrange: Declare expected metric emissions
        expected_project = "projects/arvo-eng-prd"
        expected_labels = {"partner": "porto", "approved": "true"}

        expected_calls = [
            # First metric: total value (300 + 400 + 300 = 1000.0)
            call(
                name=expected_project,
                time_series=MetricMatcher(
                    metric_type="claims/pipeline/filtered_pre/vl_pago/total",
                    value=1000.0,
                    labels=expected_labels,
                ),
            ),
            # Second metric: relative value (1000.0 / 4000.0 = 0.25)
            call(
                name=expected_project,
                time_series=MetricMatcher(
                    metric_type="claims/pipeline/filtered_pre/vl_pago/relative",
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
def test_pre_filtered_base_handler_with_wrangling_pipeline(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
):
    """Integration test for PreFilteredBaseHandler via wrangling pipeline.

    This test:
    1. Creates BigQuery tables with test data
    2. Triggers the handler with a pipesv2_wrangling completion event
    3. Verifies both total and relative metrics are emitted correctly
    """
    dataset_id = "test_dataset"
    unprocessable_table_id = "unprocessable_claims"
    processable_table_id = "processable_claims"

    # Create dataset
    dataset = bigquery.Dataset(f"{bigquery_client.project}.{dataset_id}")
    dataset.location = "US"
    bigquery_client.create_dataset(dataset, exists_ok=True)

    try:
        # Define table schema for claims tables
        schema = [
            bigquery.SchemaField("vl_pago", "FLOAT", mode="NULLABLE"),
        ]

        # Create unprocessable claims table and insert test data
        unprocessable_table = bigquery.Table(
            f"{bigquery_client.project}.{dataset_id}.{unprocessable_table_id}", schema=schema
        )
        bigquery_client.create_table(unprocessable_table, exists_ok=True)

        # Insert test data: total vl_pago = 1000.0
        unprocessable_rows = [
            {"vl_pago": 300.0},
            {"vl_pago": 400.0},
            {"vl_pago": 300.0},
        ]
        bigquery_client.insert_rows_json(
            f"{bigquery_client.project}.{dataset_id}.{unprocessable_table_id}", unprocessable_rows
        )

        # Create processable claims table and insert test data
        processable_table = bigquery.Table(
            f"{bigquery_client.project}.{dataset_id}.{processable_table_id}", schema=schema
        )
        bigquery_client.create_table(processable_table, exists_ok=True)

        # Insert test data: total vl_pago = 4000.0
        processable_rows = [
            {"vl_pago": 1000.0},
            {"vl_pago": 1500.0},
            {"vl_pago": 1500.0},
        ]
        bigquery_client.insert_rows_json(
            f"{bigquery_client.project}.{dataset_id}.{processable_table_id}", processable_rows
        )

        # Create CloudEvent payload for pipesv2_wrangling pipeline completion
        # Wrangling events have payload directly in cloud_event.data (not base64 encoded)
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
                    "pipeline_uuid": "pipesv2_wrangling",
                    "status": "COMPLETED",
                    "variables": {
                        "partner": "abertta",
                        "refined_unprocessable_claims_output_table": (
                            f"{bigquery_client.project}.{dataset_id}.{unprocessable_table_id}"
                        ),
                        "refined_processable_claims_output_table": (
                            f"{bigquery_client.project}.{dataset_id}.{processable_table_id}"
                        ),
                    },
                },
            },
        )

        # Arrange: Declare expected metric emissions
        expected_project = "projects/arvo-eng-prd"
        expected_labels = {"partner": "abertta", "approved": "false"}

        expected_calls = [
            # First metric: total value (300 + 400 + 300 = 1000.0)
            call(
                name=expected_project,
                time_series=MetricMatcher(
                    metric_type="claims/pipeline/filtered_pre/vl_pago/total",
                    value=1000.0,
                    labels=expected_labels,
                ),
            ),
            # Second metric: relative value (1000.0 / 4000.0 = 0.25)
            call(
                name=expected_project,
                time_series=MetricMatcher(
                    metric_type="claims/pipeline/filtered_pre/vl_pago/relative",
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

