"""Integration tests for PostFilteredBaseHandler."""

import pytest
from cloudevents.http import CloudEvent
from google.cloud import bigquery
from pytest_mock import MockerFixture

import main
from tests.metrics import MetricMatcher

# Test data constants
EXCLUDED_SAVINGS_ROWS = [
    {"vl_glosa_arvo": 150.0},
    {"vl_glosa_arvo": 200.0},
    {"vl_glosa_arvo": 150.0},
]
EXCLUDED_SAVINGS_TOTAL = 500.0

SAVINGS_ROWS = [
    {"vl_glosa_arvo": 500.0},
    {"vl_glosa_arvo": 750.0},
    {"vl_glosa_arvo": 750.0},
]
SAVINGS_TOTAL = 2000.0

RELATIVE_VALUE = EXCLUDED_SAVINGS_TOTAL / SAVINGS_TOTAL  # 0.25


def _create_dataset(bigquery_client, dataset_id: str) -> None:
    """Create a BigQuery dataset for testing."""
    dataset = bigquery.Dataset(f"{bigquery_client.project}.{dataset_id}")
    dataset.location = "US"
    bigquery_client.create_dataset(dataset, exists_ok=True)


def _create_savings_table_with_data(
    bigquery_client, dataset_id: str, table_id: str, rows: list[dict]
) -> None:
    """Create a savings table and insert test data."""
    schema = [
        bigquery.SchemaField("vl_glosa_arvo", "FLOAT", mode="NULLABLE"),
    ]
    table = bigquery.Table(f"{bigquery_client.project}.{dataset_id}.{table_id}", schema=schema)
    bigquery_client.create_table(table, exists_ok=True)
    bigquery_client.insert_rows_json(f"{bigquery_client.project}.{dataset_id}.{table_id}", rows)


def _create_claims_tables(bigquery_client, dataset_id: str) -> tuple[str, str]:
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


def _create_cloud_event(
    bigquery_client,
    dataset_id: str,
    pipeline_uuid: str,
    partner: str,
    excluded_table_id: str,
    savings_table_id: str,
    excluded_table_var: str,
    savings_table_var: str,
    include_claims_tables: bool = False,
) -> CloudEvent:
    """Create a CloudEvent for pipeline completion."""
    variables = {
        "partner": partner,
        excluded_table_var: f"{bigquery_client.project}.{dataset_id}.{excluded_table_id}",
        savings_table_var: f"{bigquery_client.project}.{dataset_id}.{savings_table_id}",
    }

    if include_claims_tables:
        unprocessable_table_id, processable_table_id = _create_claims_tables(
            bigquery_client, dataset_id
        )
        variables.update(
            {
                "unprocessable_claims_input_table": (
                    f"{bigquery_client.project}.{dataset_id}.{unprocessable_table_id}"
                ),
                "processable_claims_input_table": (
                    f"{bigquery_client.project}.{dataset_id}.{processable_table_id}"
                ),
            }
        )

    return CloudEvent(
        {
            "type": "google.cloud.pubsub.topic.v1.messagePublished",
            "source": "//pubsub.googleapis.com/projects/test-project/topics/test-topic",
            "specversion": "1.0",
            "id": "test-event-id",
        },
        {
            "source_timestamp": "2024-01-15T10:30:00Z",
            "payload": {
                "pipeline_uuid": pipeline_uuid,
                "status": "COMPLETED",
                "variables": variables,
            },
        },
    )


def _create_expected_metric_calls(
    mocker: MockerFixture, partner: str, approved: str, total_value: float, relative_value: float
) -> list:
    """Create expected metric call matchers."""
    expected_project = "projects/arvo-eng-prd"
    expected_labels = {"partner": partner, "approved": approved}

    return [
        mocker.call(
            name=expected_project,
            time_series=MetricMatcher(
                metric_type="claims/pipeline/filtered_post/vl_glosa_arvo/total",
                value=total_value,
                labels=expected_labels,
            ),
        ),
        mocker.call(
            name=expected_project,
            time_series=MetricMatcher(
                metric_type="claims/pipeline/filtered_post/vl_glosa_arvo/relative",
                value=relative_value,
                labels=expected_labels,
            ),
        ),
    ]


def _assert_response_success(response) -> None:
    """Assert that the handler response indicates success."""
    assert response is not None
    status_code = response[1] if isinstance(response, tuple) else response.status_code
    assert status_code == 204


def _assert_metrics_emitted(mock_monitoring_client, expected_calls: list) -> None:
    """Assert that expected metric calls were made."""
    actual_calls = mock_monitoring_client.create_time_series.call_args_list

    for expected_call in expected_calls:
        expected_name = expected_call.kwargs["name"]
        expected_time_series_matcher = expected_call.kwargs["time_series"]

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

    _create_dataset(bigquery_client, dataset_id)

    try:
        _create_savings_table_with_data(
            bigquery_client, dataset_id, excluded_table_id, EXCLUDED_SAVINGS_ROWS
        )
        _create_savings_table_with_data(bigquery_client, dataset_id, savings_table_id, SAVINGS_ROWS)

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            pipeline_uuid="pipesv2_selection",
            partner="athena",
            excluded_table_id=excluded_table_id,
            savings_table_id=savings_table_id,
            excluded_table_var="excluded_savings_output_table",
            savings_table_var="savings_input_table",
        )

        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="athena",
            approved="false",
            total_value=EXCLUDED_SAVINGS_TOTAL,
            relative_value=RELATIVE_VALUE,
        )

        response = main.handle_cloud_event(event)

        _assert_response_success(response)
        _assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_post_filtered_base_handler_missing_excluded_table(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
):
    """Integration test for PostFilteredBaseHandler when excluded table doesn't exist.

    This test:
    1. Creates only the savings table (excluded table doesn't exist)
    2. Triggers the handler with a pipesv2_approval completion event
    3. Verifies that excluded sum is assumed to be 0
    4. Verifies both total and relative metrics are emitted correctly
    """
    dataset_id = "test_dataset"
    excluded_table_id = "excluded_savings"
    savings_table_id = "savings"

    _create_dataset(bigquery_client, dataset_id)

    try:
        # Only create savings table, not excluded table
        _create_savings_table_with_data(bigquery_client, dataset_id, savings_table_id, SAVINGS_ROWS)

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            pipeline_uuid="pipesv2_approval",
            partner="cemig",
            excluded_table_id=excluded_table_id,  # This table doesn't exist
            savings_table_id=savings_table_id,
            excluded_table_var="excluded_savings_input_table",
            savings_table_var="savings_input_table",
            include_claims_tables=True,
        )

        # Excluded sum should be 0 (table doesn't exist)
        # Relative value should be 0 / 2000 = 0
        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="cemig",
            approved="true",
            total_value=0.0,  # Excluded table doesn't exist, so sum is 0
            relative_value=0.0,  # 0 / 2000 = 0
        )

        response = main.handle_cloud_event(event)

        _assert_response_success(response)
        _assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
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

    _create_dataset(bigquery_client, dataset_id)

    try:
        _create_savings_table_with_data(
            bigquery_client, dataset_id, excluded_table_id, EXCLUDED_SAVINGS_ROWS
        )
        _create_savings_table_with_data(bigquery_client, dataset_id, savings_table_id, SAVINGS_ROWS)

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            pipeline_uuid="pipesv2_approval",
            partner="cemig",
            excluded_table_id=excluded_table_id,
            savings_table_id=savings_table_id,
            excluded_table_var="excluded_savings_input_table",
            savings_table_var="savings_input_table",
            include_claims_tables=True,
        )

        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="cemig",
            approved="true",
            total_value=EXCLUDED_SAVINGS_TOTAL,
            relative_value=RELATIVE_VALUE,
        )

        response = main.handle_cloud_event(event)

        _assert_response_success(response)
        _assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
