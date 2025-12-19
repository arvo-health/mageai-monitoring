"""Integration tests for SavingsBaseHandler."""

import pytest
from cloudevents.http import CloudEvent
from google.cloud import bigquery
from pytest_mock import MockerFixture

from handlers.savings_approval import SavingsApprovalHandler
from handlers.savings_evaluation import SavingsEvaluationHandler
from tests.bigquery import create_dataset
from tests.conftest import assert_response_success
from tests.metrics import MetricMatcher, assert_metrics_emitted

# Test data constants
SAVINGS_ROWS = [
    {"agent_id": "C035", "vl_glosa_arvo": 100.0},
    {"agent_id": "C035", "vl_glosa_arvo": 200.0},
    {"agent_id": "R001", "vl_glosa_arvo": 300.0},
]

C035_AMOUNT = 300.0  # 100 + 200
C035_COUNT = 2
R001_AMOUNT = 300.0
R001_COUNT = 1


def create_savings_table_with_agent_id(
    bigquery_client, dataset_id: str, table_id: str, rows: list[dict]
) -> None:
    """Create a savings table with agent_id and vl_glosa_arvo columns.

    Inserts test data into the created table.
    """
    schema = [
        bigquery.SchemaField("agent_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("vl_glosa_arvo", "FLOAT", mode="NULLABLE"),
    ]
    table = bigquery.Table(f"{bigquery_client.project}.{dataset_id}.{table_id}", schema=schema)
    bigquery_client.create_table(table, exists_ok=True)
    bigquery_client.insert_rows_json(f"{bigquery_client.project}.{dataset_id}.{table_id}", rows)


def _create_cloud_event(
    bigquery_client,
    dataset_id: str,
    pipeline_uuid: str,
    partner: str,
    savings_table_id: str,
    savings_table_var: str = "savings_output_table",
) -> CloudEvent:
    """Create a CloudEvent for pipeline completion."""
    variables = {
        "partner": partner,
        savings_table_var: f"{bigquery_client.project}.{dataset_id}.{savings_table_id}",
    }

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
    mocker: MockerFixture,
    partner: str,
    approved: str,
    agent_metrics: list[tuple[str, float, float]],
) -> list:
    """Create expected metric call matchers for multiple agent_ids.

    Args:
        mocker: Mocker fixture
        partner: Partner name
        approved: Approved value ("true" or "false")
        agent_metrics: List of tuples (agent_id, amount, count)

    Returns:
        List of expected metric call matchers
    """
    expected_project = "projects/arvo-eng-prd"
    expected_calls = []

    for agent_id, amount, count in agent_metrics:
        expected_labels = {
            "partner": partner,
            "agent_id": agent_id,
            "approved": approved,
        }
        # Add amount metric
        expected_calls.append(
            mocker.call(
                name=expected_project,
                time_series=MetricMatcher(
                    metric_type="claims/pipeline/savings/vl_glosa_arvo",
                    value=amount,
                    labels=expected_labels,
                ),
            )
        )
        # Add count metric
        expected_calls.append(
            mocker.call(
                name=expected_project,
                time_series=MetricMatcher(
                    metric_type="claims/pipeline/savings/count",
                    value=float(count),
                    labels=expected_labels,
                ),
            )
        )

    return expected_calls


@pytest.mark.integration
def test_savings_base_handler_with_evaluation_pipeline(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for SavingsBaseHandler via evaluation pipeline.

    This test:
    1. Creates BigQuery table with test data
    2. Triggers the handler with a pipesv2_evaluation completion event
    3. Verifies both amount and count metrics are emitted correctly per agent_id
    """
    dataset_id = "test_dataset"
    savings_table_id = "savings"

    create_dataset(bigquery_client, dataset_id)

    try:
        create_savings_table_with_agent_id(
            bigquery_client, dataset_id, savings_table_id, SAVINGS_ROWS
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            pipeline_uuid="pipesv2_evaluation",
            partner="cemig",
            savings_table_id=savings_table_id,
        )

        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="cemig",
            approved="false",
            agent_metrics=[
                ("C035", C035_AMOUNT, C035_COUNT),
                ("R001", R001_AMOUNT, R001_COUNT),
            ],
        )

        response = dispatch_event(event, [SavingsEvaluationHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_savings_base_handler_with_approval_pipeline(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for SavingsBaseHandler via approval pipeline.

    This test:
    1. Creates BigQuery table with test data
    2. Triggers the handler with a pipesv2_approval completion event
    3. Verifies both amount and count metrics are emitted correctly per agent_id
    """
    dataset_id = "test_dataset"
    savings_table_id = "savings"

    create_dataset(bigquery_client, dataset_id)

    try:
        create_savings_table_with_agent_id(
            bigquery_client, dataset_id, savings_table_id, SAVINGS_ROWS
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            pipeline_uuid="pipesv2_approval",
            partner="unimed_guarulhos",
            savings_table_id=savings_table_id,
            savings_table_var="savings_input_table",
        )

        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="unimed_guarulhos",
            approved="true",
            agent_metrics=[
                ("C035", C035_AMOUNT, C035_COUNT),
                ("R001", R001_AMOUNT, R001_COUNT),
            ],
        )

        response = dispatch_event(event, [SavingsApprovalHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_savings_base_handler_missing_table(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for SavingsBaseHandler when table doesn't exist.

    This test:
    1. Creates a dataset but no table
    2. Triggers the handler with a pipesv2_evaluation completion event
    3. Verifies no metrics are emitted
    """
    dataset_id = "test_dataset"
    savings_table_id = "nonexistent_table"

    create_dataset(bigquery_client, dataset_id)

    try:
        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            pipeline_uuid="pipesv2_evaluation",
            partner="cemig",
            savings_table_id=savings_table_id,
        )

        response = dispatch_event(event, [SavingsEvaluationHandler])

        assert_response_success(response)
        # Verify no metrics were emitted
        assert mock_monitoring_client.create_time_series.call_count == 0

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_savings_base_handler_empty_table(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for SavingsBaseHandler with empty table.

    This test:
    1. Creates empty BigQuery table
    2. Triggers the handler with a pipesv2_approval completion event
    3. Verifies no metrics are emitted (empty result set)
    """
    dataset_id = "test_dataset"
    savings_table_id = "savings"

    create_dataset(bigquery_client, dataset_id)

    try:
        # Create empty table
        create_savings_table_with_agent_id(bigquery_client, dataset_id, savings_table_id, [])

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            pipeline_uuid="pipesv2_approval",
            partner="unimed_guarulhos",
            savings_table_id=savings_table_id,
            savings_table_var="savings_input_table",
        )

        response = dispatch_event(event, [SavingsApprovalHandler])

        assert_response_success(response)
        # Verify no metrics were emitted
        assert mock_monitoring_client.create_time_series.call_count == 0

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
