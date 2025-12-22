"""Unit and integration tests for SelectedSavingsApprovalHandler."""

import pytest
from cloudevents.http import CloudEvent
from google.cloud import bigquery
from pytest_mock import MockerFixture

from handlers.selected_savings_approval import SelectedSavingsApprovalHandler
from tests.bigquery import create_dataset
from tests.conftest import assert_response_success
from tests.metrics import MetricMatcher, assert_metrics_emitted

# Test data constants
SAVINGS_ROWS = [
    {"agent_id": "agent_1", "vl_glosa_arvo": 100.0},
    {"agent_id": "agent_1", "vl_glosa_arvo": 200.0},
    {"agent_id": "agent_2", "vl_glosa_arvo": 300.0},
    {"agent_id": "agent_2", "vl_glosa_arvo": 150.0},
    {"agent_id": "agent_3", "vl_glosa_arvo": 50.0},
]

AGENT_1_TOTAL = 300.0  # 100 + 200
AGENT_2_TOTAL = 450.0  # 300 + 150
AGENT_3_TOTAL = 50.0


def create_selected_savings_table_with_data(
    bigquery_client, dataset_id: str, table_id: str, rows: list[dict]
) -> None:
    """Create a selected savings table with agent_id and vl_glosa_arvo columns.

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
    partner: str,
    selected_savings_table_id: str,
) -> CloudEvent:
    """Create a CloudEvent for pipeline completion."""
    variables = {
        "partner": partner,
        "selected_savings_input_table": (
            f"{bigquery_client.project}.{dataset_id}.{selected_savings_table_id}"
        ),
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
                "pipeline_uuid": "pipesv2_approval",
                "status": "COMPLETED",
                "variables": variables,
            },
        },
    )


def _create_expected_metric_calls(
    mocker: MockerFixture, partner: str, agent_metrics: list[tuple[str, float]]
) -> list:
    """Create expected metric call matchers for multiple agent_ids."""
    expected_project = "projects/arvo-eng-prd"
    expected_calls = []

    for agent_id, total_value in agent_metrics:
        expected_labels = {
            "partner": partner,
            "agent_id": agent_id,
            "source": "selected_savings",
        }
        expected_calls.append(
            mocker.call(
                name=expected_project,
                time_series=MetricMatcher(
                    metric_type="claims/glosa/approved/vl_glosa_arvo",
                    value=total_value,
                    labels=expected_labels,
                ),
            )
        )

    return expected_calls


@pytest.mark.parametrize(
    "decoded_message,expected",
    [
        (
            {
                "payload": {
                    "pipeline_uuid": "pipesv2_approval",
                    "status": "COMPLETED",
                }
            },
            True,
        ),
        (
            {
                "payload": {
                    "pipeline_uuid": "pipesv2_wrangling",
                    "status": "COMPLETED",
                }
            },
            False,
        ),
        (
            {
                "payload": {
                    "pipeline_uuid": "pipesv2_approval",
                    "status": "RUNNING",
                }
            },
            False,
        ),
        ({}, False),
    ],
)
def test_match(mocker: MockerFixture, decoded_message, expected):
    """Test that match returns the expected result for various message configurations."""
    handler = SelectedSavingsApprovalHandler(
        monitoring_client=mocker.MagicMock(),
        bq_client=mocker.MagicMock(),
        run_project_id="test-project",
        data_project_id="test-data-project",
    )

    assert handler.match(decoded_message) is expected


def test_handle_queries_and_emits_metrics(mocker: MockerFixture):
    """Test that handle queries BigQuery and emits metrics for each agent_id."""
    mock_bq_client = mocker.MagicMock()
    mock_monitoring_client = mocker.MagicMock()

    # Mock BigQuery query result
    mock_row1 = mocker.MagicMock()
    mock_row1.agent_id = "agent_1"
    mock_row1.total_vl_glosa_arvo = 300.0

    mock_row2 = mocker.MagicMock()
    mock_row2.agent_id = "agent_2"
    mock_row2.total_vl_glosa_arvo = 450.0

    mock_result = mocker.MagicMock()
    mock_result.__iter__ = mocker.Mock(return_value=iter([mock_row1, mock_row2]))

    mock_query_job = mocker.MagicMock()
    mock_query_job.result.return_value = mock_result
    mock_bq_client.query.return_value = mock_query_job

    handler = SelectedSavingsApprovalHandler(
        monitoring_client=mock_monitoring_client,
        bq_client=mock_bq_client,
        run_project_id="test-project",
        data_project_id="test-data-project",
    )

    decoded_message = {
        "source_timestamp": "2024-01-15T10:30:00Z",
        "payload": {
            "pipeline_uuid": "pipesv2_approval",
            "status": "COMPLETED",
            "variables": {
                "partner": "porto",
                "selected_savings_input_table": "dataset.selected_savings",
            },
        },
    }

    # Mock emit_gauge_metric
    mock_emit = mocker.patch("handlers.selected_savings_approval.emit_gauge_metric")

    handler.handle(decoded_message)

    # Verify query was called with correct SQL
    mock_bq_client.query.assert_called_once()
    query_call = mock_bq_client.query.call_args[0][0]
    assert "SELECT agent_id, COALESCE(SUM(vl_glosa_arvo), 0) AS total_vl_glosa_arvo" in query_call
    assert "FROM `dataset.selected_savings`" in query_call
    assert "GROUP BY agent_id" in query_call

    # Verify metrics were emitted for each agent
    assert mock_emit.call_count == 2

    # Verify first metric call
    first_call = mock_emit.call_args_list[0]
    assert first_call.kwargs["name"] == "claims/glosa/approved/vl_glosa_arvo"
    assert first_call.kwargs["value"] == 300.0
    assert first_call.kwargs["labels"] == {
        "partner": "porto",
        "agent_id": "agent_1",
        "source": "selected_savings",
    }

    # Verify second metric call
    second_call = mock_emit.call_args_list[1]
    assert second_call.kwargs["name"] == "claims/glosa/approved/vl_glosa_arvo"
    assert second_call.kwargs["value"] == 450.0
    assert second_call.kwargs["labels"] == {
        "partner": "porto",
        "agent_id": "agent_2",
        "source": "selected_savings",
    }


@pytest.mark.integration
def test_selected_savings_approval_handler_with_multiple_agents(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for SelectedSavingsApprovalHandler with multiple agent_ids.

    This test:
    1. Creates BigQuery table with agent_id and vl_glosa_arvo columns
    2. Inserts test data with multiple agent_ids
    3. Triggers the handler with a pipesv2_approval completion event
    4. Verifies multiple metric points are emitted (one per agent_id)
    5. Verifies labels and values are correct
    """
    dataset_id = "test_dataset"
    selected_savings_table_id = "selected_savings"

    create_dataset(bigquery_client, dataset_id)

    try:
        create_selected_savings_table_with_data(
            bigquery_client, dataset_id, selected_savings_table_id, SAVINGS_ROWS
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            partner="porto",
            selected_savings_table_id=selected_savings_table_id,
        )

        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="porto",
            agent_metrics=[
                ("agent_1", AGENT_1_TOTAL),
                ("agent_2", AGENT_2_TOTAL),
                ("agent_3", AGENT_3_TOTAL),
            ],
        )

        response = dispatch_event(event, [SelectedSavingsApprovalHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_selected_savings_approval_handler_empty_table(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for SelectedSavingsApprovalHandler with empty table.

    This test:
    1. Creates empty BigQuery table
    2. Triggers the handler with a pipesv2_approval completion event
    3. Verifies no metrics are emitted (empty result set)
    """
    dataset_id = "test_dataset"
    selected_savings_table_id = "selected_savings"

    create_dataset(bigquery_client, dataset_id)

    try:
        # Create empty table
        create_selected_savings_table_with_data(
            bigquery_client, dataset_id, selected_savings_table_id, []
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            partner="cemig",
            selected_savings_table_id=selected_savings_table_id,
        )

        response = dispatch_event(event, [SelectedSavingsApprovalHandler])

        assert_response_success(response)
        # Verify no metrics were emitted
        assert mock_monitoring_client.create_time_series.call_count == 0

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_selected_savings_approval_handler_table_not_found(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for SelectedSavingsApprovalHandler when table doesn't exist.

    This test:
    1. Creates a dataset but no table
    2. Triggers the handler with a pipesv2_approval completion event
    3. Verifies no metrics are emitted
    """
    dataset_id = "test_dataset"
    selected_savings_table_id = "nonexistent_table"

    create_dataset(bigquery_client, dataset_id)

    try:
        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            partner="porto",
            selected_savings_table_id=selected_savings_table_id,
        )

        response = dispatch_event(event, [SelectedSavingsApprovalHandler])

        assert_response_success(response)
        # Verify no metrics were emitted
        assert mock_monitoring_client.create_time_series.call_count == 0

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
