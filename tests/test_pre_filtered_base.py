"""Integration tests for PreFilteredBaseHandler."""

import pytest
from cloudevents.http import CloudEvent
from google.cloud import bigquery
from pytest_mock import MockerFixture

import main
from tests.metrics import MetricMatcher

# Test data constants
UNPROCESSABLE_CLAIMS_ROWS = [
    {"vl_pago": 300.0},
    {"vl_pago": 400.0},
    {"vl_pago": 300.0},
]
UNPROCESSABLE_CLAIMS_TOTAL = 1000.0

PROCESSABLE_CLAIMS_ROWS = [
    {"vl_pago": 1000.0},
    {"vl_pago": 1500.0},
    {"vl_pago": 1500.0},
]
PROCESSABLE_CLAIMS_TOTAL = 4000.0

RELATIVE_VALUE = UNPROCESSABLE_CLAIMS_TOTAL / (
    UNPROCESSABLE_CLAIMS_TOTAL + PROCESSABLE_CLAIMS_TOTAL
)  # 0.2


def _create_dataset(bigquery_client, dataset_id: str) -> None:
    """Create a BigQuery dataset for testing."""
    dataset = bigquery.Dataset(f"{bigquery_client.project}.{dataset_id}")
    dataset.location = "US"
    bigquery_client.create_dataset(dataset, exists_ok=True)


def _create_claims_table_with_data(
    bigquery_client, dataset_id: str, table_id: str, rows: list[dict]
) -> None:
    """Create a claims table and insert test data."""
    schema = [
        bigquery.SchemaField("vl_pago", "FLOAT", mode="NULLABLE"),
    ]
    table = bigquery.Table(f"{bigquery_client.project}.{dataset_id}.{table_id}", schema=schema)
    bigquery_client.create_table(table, exists_ok=True)
    bigquery_client.insert_rows_json(f"{bigquery_client.project}.{dataset_id}.{table_id}", rows)


def _create_savings_tables(bigquery_client, dataset_id: str) -> tuple[str, str]:
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


def _create_cloud_event(
    bigquery_client,
    dataset_id: str,
    pipeline_uuid: str,
    partner: str,
    unprocessable_table_id: str,
    processable_table_id: str,
    unprocessable_table_var: str,
    processable_table_var: str,
    include_savings_tables: bool = False,
) -> CloudEvent:
    """Create a CloudEvent for pipeline completion."""
    variables = {
        "partner": partner,
        unprocessable_table_var: (
            f"{bigquery_client.project}.{dataset_id}.{unprocessable_table_id}"
        ),
        processable_table_var: (f"{bigquery_client.project}.{dataset_id}.{processable_table_id}"),
    }

    if include_savings_tables:
        excluded_table_id, savings_table_id = _create_savings_tables(bigquery_client, dataset_id)
        variables.update(
            {
                "excluded_savings_input_table": (
                    f"{bigquery_client.project}.{dataset_id}.{excluded_table_id}"
                ),
                "savings_input_table": (
                    f"{bigquery_client.project}.{dataset_id}.{savings_table_id}"
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
                metric_type="claims/pipeline/filtered_pre/vl_pago/total",
                value=total_value,
                labels=expected_labels,
            ),
        ),
        mocker.call(
            name=expected_project,
            time_series=MetricMatcher(
                metric_type="claims/pipeline/filtered_pre/vl_pago/relative",
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
def test_pre_filtered_base_handler_with_approval_pipeline(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
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

    _create_dataset(bigquery_client, dataset_id)

    try:
        _create_claims_table_with_data(
            bigquery_client, dataset_id, unprocessable_table_id, UNPROCESSABLE_CLAIMS_ROWS
        )
        _create_claims_table_with_data(
            bigquery_client, dataset_id, processable_table_id, PROCESSABLE_CLAIMS_ROWS
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            pipeline_uuid="pipesv2_approval",
            partner="porto",
            unprocessable_table_id=unprocessable_table_id,
            processable_table_id=processable_table_id,
            unprocessable_table_var="unprocessable_claims_input_table",
            processable_table_var="processable_claims_input_table",
            include_savings_tables=True,
        )

        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="porto",
            approved="true",
            total_value=UNPROCESSABLE_CLAIMS_TOTAL,
            relative_value=RELATIVE_VALUE,
        )

        response = main.handle_cloud_event(event)

        _assert_response_success(response)
        _assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_pre_filtered_base_handler_with_wrangling_pipeline(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
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

    _create_dataset(bigquery_client, dataset_id)

    try:
        _create_claims_table_with_data(
            bigquery_client, dataset_id, unprocessable_table_id, UNPROCESSABLE_CLAIMS_ROWS
        )
        _create_claims_table_with_data(
            bigquery_client, dataset_id, processable_table_id, PROCESSABLE_CLAIMS_ROWS
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            pipeline_uuid="pipesv2_wrangling",
            partner="abertta",
            unprocessable_table_id=unprocessable_table_id,
            processable_table_id=processable_table_id,
            unprocessable_table_var="refined_unprocessable_claims_output_table",
            processable_table_var="refined_processable_claims_output_table",
        )

        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="abertta",
            approved="false",
            total_value=UNPROCESSABLE_CLAIMS_TOTAL,
            relative_value=RELATIVE_VALUE,
        )

        response = main.handle_cloud_event(event)

        _assert_response_success(response)
        _assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
