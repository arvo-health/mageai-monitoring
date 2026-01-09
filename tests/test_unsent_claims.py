"""Unit and integration tests for UnsentClaimsHandler."""

from datetime import datetime, timedelta

import pytest
from cloudevents.http import CloudEvent
from google.cloud import bigquery
from pytest_mock import MockerFixture

from handlers.unsent_claims import UnsentClaimsHandler
from tests.bigquery import create_dataset
from tests.conftest import assert_response_success
from tests.metrics import MetricMatcher, assert_metrics_emitted


def _create_cloud_event(
    bigquery_client,
    dataset_id: str,
    partner: str,
    processable_table_id: str,
    unprocessable_table_id: str,
    internal_validation_table_id: str,
    submitted_table_id: str,
    submission_run_id: str,
    source_timestamp: str,
) -> CloudEvent:
    """Create a CloudEvent for pipeline completion."""
    variables = {
        "partner": partner,
        "submission_run_id": submission_run_id,
        "processable_claims_historical_table": (
            f"{bigquery_client.project}.{dataset_id}.{processable_table_id}"
        ),
        "unprocessable_claims_historical_table": (
            f"{bigquery_client.project}.{dataset_id}.{unprocessable_table_id}"
        ),
        "claims_submitted_output_table": (
            f"{bigquery_client.project}.{dataset_id}.{submitted_table_id}"
        ),
        "internal_validation_output_table": (
            f"{bigquery_client.project}.{dataset_id}.{internal_validation_table_id}"
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
            "source_timestamp": source_timestamp,
            "payload": {
                "pipeline_uuid": "pipesv2_submission",
                "status": "COMPLETED",
                "variables": variables,
            },
        },
    )


def _create_expected_metric_calls(
    mocker: MockerFixture,
    partner: str,
    values: list[dict],
) -> list:
    """Create expected metric call matchers."""
    expected_project = "projects/arvo-eng-prd"

    calls = []
    for value in values:
        calls.append(
            mocker.call(
                name=expected_project,
                time_series=MetricMatcher(
                    metric_type="claims/pipeline/claims/vl_pago/sent_over_recv_last_2_days",
                    value=value["perc_pago"],
                    labels={"partner": partner, "status": value["status"]},
                ),
            ),
        )

        calls.append(
            mocker.call(
                name=expected_project,
                time_series=MetricMatcher(
                    metric_type="claims/pipeline/claims/vl_info/sent_over_recv_last_2_days",
                    value=value["perc_info"],
                    labels={"partner": partner, "status": value["status"]},
                ),
            ),
        )

    return calls


def create_claims_table_with_data(
    bigquery_client, dataset_id: str, table_id: str, rows: list[dict]
) -> None:
    """Create an unsent claims table with id_arvo, run_id, vl_pago, vl_info, ingested_at columns.

    Inserts test data into the created table.
    """
    schema = [
        bigquery.SchemaField("id_arvo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("vl_pago", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("vl_info", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="NULLABLE"),
    ]
    table = bigquery.Table(f"{bigquery_client.project}.{dataset_id}.{table_id}", schema=schema)
    bigquery_client.create_table(table, exists_ok=True)
    bigquery_client.insert_rows_json(f"{bigquery_client.project}.{dataset_id}.{table_id}", rows)


def create_internal_validation_table_with_data(
    bigquery_client, dataset_id: str, table_id: str, rows: list[dict]
) -> None:
    """Create an internal validation table with id_arvo, ingested_at, and status columns.

    Inserts test data into the created table.
    """
    schema = [
        bigquery.SchemaField("id_arvo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("id_fatura", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
    ]
    table = bigquery.Table(f"{bigquery_client.project}.{dataset_id}.{table_id}", schema=schema)
    bigquery_client.create_table(table, exists_ok=True)
    bigquery_client.insert_rows_json(f"{bigquery_client.project}.{dataset_id}.{table_id}", rows)


def create_submitted_claims_table_with_data(
    bigquery_client, dataset_id: str, table_id: str, rows: list[dict]
) -> None:
    """Create a submitted claims table with id_arvo, run_id columns.

    Inserts test data into the created table.
    """
    schema = [
        bigquery.SchemaField("id_arvo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("submission_run_id", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
    ]
    table = bigquery.Table(f"{bigquery_client.project}.{dataset_id}.{table_id}", schema=schema)
    bigquery_client.create_table(table, exists_ok=True)
    bigquery_client.insert_rows_json(f"{bigquery_client.project}.{dataset_id}.{table_id}", rows)


@pytest.mark.parametrize(
    "decoded_message,expected",
    [
        (
            {
                "payload": {
                    "pipeline_uuid": "pipesv2_submission",
                    "status": "COMPLETED",
                }
            },
            True,
        ),
        (
            {
                "payload": {
                    "pipeline_uuid": "pipesv2_approval",
                    "status": "COMPLETED",
                }
            },
            False,
        ),
        (
            {
                "payload": {
                    "pipeline_uuid": "pipesv2_submission",
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
    handler = UnsentClaimsHandler(
        monitoring_client=mocker.MagicMock(),
        bq_client=mocker.MagicMock(),
        run_project_id="test-project",
        data_project_id="test-data-project",
    )

    assert handler.match(decoded_message) is expected


@pytest.mark.integration
def test_handle_queries_and_emits_metrics(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Test that handle queries BigQuery and emits metrics for the sum of vl_pago and vl_info
    of unsent claims."""

    dataset_id = "test_dataset"
    processable_table_id = "processable_claims"
    unprocessable_table_id = "unprocessable_claims"
    internal_validation_table_id = "internal_validation"
    submitted_table_id = "submitted_claims"
    submission_run_id = "run_X"
    source_timestamp = "2024-01-15T10:30:00Z"

    create_dataset(bigquery_client, dataset_id)

    try:
        # The most recent ingested_at of the submitted claims for the submission_run_id
        submitted_ingested_at = datetime.fromisoformat("2024-01-14T20:00:00Z")

        # Latest timestamp (within the range)
        submitted_ingested_at_str = submitted_ingested_at.isoformat()
        # Valid ingested at (within the range)
        one_day_before_str = (submitted_ingested_at - timedelta(days=1)).isoformat()
        # Old ingested at (outside the range)
        three_days_before_str = (submitted_ingested_at - timedelta(days=3)).isoformat()
        # Recent ingested at (outside the range)
        five_minutes_after_str = (submitted_ingested_at + timedelta(minutes=5)).isoformat()

        processable_rows = [
            {
                "id_arvo": "p1",
                "vl_pago": 1000.0,
                "vl_info": 1000.0,
                "ingested_at": three_days_before_str,  # Ignored (date range)
            },
            {
                "id_arvo": "p2",  # Missing from submitted claims
                "vl_pago": 50.0,
                "vl_info": 100.0,
                "ingested_at": one_day_before_str,
            },
            {
                "id_arvo": "p3",
                "vl_pago": 300.0,
                "vl_info": 300.0,
                "ingested_at": one_day_before_str,
            },
            {
                "id_arvo": "p4",
                "vl_pago": 50.0,
                "vl_info": 100.0,
                "ingested_at": submitted_ingested_at_str,
            },
            {
                "id_arvo": "p5",  # Missing from submitted claims
                "vl_pago": 50.0,
                "vl_info": 50.0,
                "ingested_at": submitted_ingested_at_str,
            },
            {
                "id_arvo": "p6",  # Ignored (claim pending validation)
                "vl_pago": 550.0,
                "vl_info": 450.0,
                "ingested_at": submitted_ingested_at_str,
            },
            {
                "id_arvo": "p7",  # Ignored (invoice pending validation)
                "vl_pago": 550.0,
                "vl_info": 450.0,
                "ingested_at": submitted_ingested_at_str,
            },
            {
                "id_arvo": "p8",
                "vl_pago": 1000.0,
                "vl_info": 1000.0,
                "ingested_at": five_minutes_after_str,  # Ignored (date range)
            },
            {
                "id_arvo": "p9",
                "vl_pago": 1000.0,
                "vl_info": 1000.0,
                "ingested_at": five_minutes_after_str,  # Ignored (date range)
            },
        ]

        unprocessable_rows = [
            {
                "id_arvo": "u1",
                "vl_pago": 1000.0,
                "vl_info": 1000.0,
                "ingested_at": three_days_before_str,  # Ignored (date range)
            },
            {
                "id_arvo": "u2",
                "vl_pago": 200.0,
                "vl_info": 250.0,
                "ingested_at": one_day_before_str,
            },
            {
                "id_arvo": "u3",  # Missing from submitted claims
                "vl_pago": 150.0,
                "vl_info": 200.0,
                "ingested_at": submitted_ingested_at_str,
            },
            {
                "id_arvo": "u4",
                "vl_pago": 1000.0,
                "vl_info": 1000.0,
                "ingested_at": five_minutes_after_str,  # Ignored (date range)
            },
        ]

        submitted_rows = [
            {
                "id_arvo": "p3",
                "submission_run_id": submission_run_id,
                "ingested_at": one_day_before_str,
                "status": "SUBMITTED_SUCCESS",
            },
            {
                "id_arvo": "p4",
                "submission_run_id": submission_run_id,
                "ingested_at": submitted_ingested_at_str,
                "status": "SUBMITTED_ERROR",
            },
            {
                "id_arvo": "u2",
                "submission_run_id": submission_run_id,
                "ingested_at": one_day_before_str,
                "status": "RETRY",
            },
        ]

        internal_validation_rows = [
            {
                "id_arvo": "p4",
                "ingested_at": submitted_ingested_at_str,
                "id_fatura": "f1",
                "status": "DENIED",
            },
            {
                "id_arvo": "p5",
                "ingested_at": submitted_ingested_at_str,
                "id_fatura": "f1",
                "status": "APPROVED",
            },
            {
                "id_arvo": "p6",
                "ingested_at": submitted_ingested_at_str,
                "id_fatura": "f2",
                "status": "SENT_FOR_VALIDATION",
            },
            {
                "id_arvo": "p7",
                "ingested_at": submitted_ingested_at_str,
                "id_fatura": "f2",
                "status": "APPROVED",
            },
        ]

        create_claims_table_with_data(
            bigquery_client, dataset_id, processable_table_id, rows=processable_rows
        )
        create_claims_table_with_data(
            bigquery_client, dataset_id, unprocessable_table_id, rows=unprocessable_rows
        )
        create_submitted_claims_table_with_data(
            bigquery_client, dataset_id, submitted_table_id, rows=submitted_rows
        )
        create_internal_validation_table_with_data(
            bigquery_client, dataset_id, internal_validation_table_id, rows=internal_validation_rows
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            partner="porto",
            processable_table_id=processable_table_id,
            unprocessable_table_id=unprocessable_table_id,
            internal_validation_table_id=internal_validation_table_id,
            submitted_table_id=submitted_table_id,
            submission_run_id=submission_run_id,
            source_timestamp=source_timestamp,
        )

        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="porto",
            values=[
                {
                    "status": "RETRY",
                    "perc_pago": 0.25,
                    "perc_info": 0.25,
                },
                {
                    "status": "SUBMITTED_ERROR",
                    "perc_pago": 0.0625,
                    "perc_info": 0.1,
                },
                {
                    "status": "SUBMITTED_SUCCESS",
                    "perc_pago": 0.375,
                    "perc_info": 0.3,
                },
            ],
        )

        response = dispatch_event(event, [UnsentClaimsHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)
    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_handle_queries_and_emits_metrics_no_submitted_claims(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Test that handle queries BigQuery and emits metrics for the sum of vl_pago and vl_info
    of unsent claims when there are no submitted claims within the last 2 days.

    In this case, we should emit 0% for the success metric as nothing was submitted.
    """

    dataset_id = "test_dataset"
    processable_table_id = "processable_claims"
    unprocessable_table_id = "unprocessable_claims"
    internal_validation_table_id = "internal_validation"
    submitted_table_id = "submitted_claims"
    submission_run_id = "run_X"
    source_timestamp = "2024-01-15T10:30:00Z"

    create_dataset(bigquery_client, dataset_id)

    try:
        # The fallback timestamp is the source_timestamp minus 20 minutes
        submitted_ingested_at = datetime.fromisoformat(source_timestamp) - timedelta(minutes=20)

        # Latest ingested at (within the range)
        submitted_ingested_at_str = submitted_ingested_at.isoformat()
        # Valid ingested at (within the range)
        one_day_before_str = (submitted_ingested_at - timedelta(days=1)).isoformat()
        # Old ingested at (outside the range)
        three_days_before_str = (submitted_ingested_at - timedelta(days=3)).isoformat()
        # Recent ingested at (outside the range)
        five_minutes_after_str = (submitted_ingested_at + timedelta(minutes=5)).isoformat()

        processable_rows = [
            {
                "id_arvo": "p1",
                "vl_pago": 1000.0,
                "vl_info": 1000.0,
                "ingested_at": three_days_before_str,  # Ignored
            },
            {
                "id_arvo": "p2",  # Missing from submitted claims
                "vl_pago": 50.0,
                "vl_info": 100.0,
                "ingested_at": one_day_before_str,
            },
            {
                "id_arvo": "p3",
                "vl_pago": 300.0,
                "vl_info": 300.0,
                "ingested_at": one_day_before_str,
            },
            {
                "id_arvo": "p4",
                "vl_pago": 50.0,
                "vl_info": 100.0,
                "ingested_at": submitted_ingested_at_str,
            },
            {
                "id_arvo": "p5",  # Missing from submitted claims
                "vl_pago": 50.0,
                "vl_info": 50.0,
                "ingested_at": submitted_ingested_at_str,
            },
            {
                "id_arvo": "p6",
                "vl_pago": 1000.0,
                "vl_info": 1000.0,
                "ingested_at": five_minutes_after_str,  # Ignored
            },
            {
                "id_arvo": "p7",
                "vl_pago": 1000.0,
                "vl_info": 1000.0,
                "ingested_at": five_minutes_after_str,  # Ignored
            },
        ]

        unprocessable_rows = [
            {
                "id_arvo": "u1",
                "vl_pago": 1000.0,
                "vl_info": 1000.0,
                "ingested_at": three_days_before_str,  # Ignored
            },
            {
                "id_arvo": "u2",
                "vl_pago": 200.0,
                "vl_info": 250.0,
                "ingested_at": one_day_before_str,
            },
            {
                "id_arvo": "u3",  # Missing from submitted claims
                "vl_pago": 150.0,
                "vl_info": 200.0,
                "ingested_at": submitted_ingested_at_str,
            },
            {
                "id_arvo": "u4",
                "vl_pago": 1000.0,
                "vl_info": 1000.0,
                "ingested_at": five_minutes_after_str,  # Ignored
            },
        ]

        submitted_rows = [
            {
                "id_arvo": "p1",
                "submission_run_id": "run_A",
                "ingested_at": three_days_before_str,
                "status": "SUBMITTED_SUCCESS",
            },
            {
                "id_arvo": "u1",
                "submission_run_id": "run_A",
                "ingested_at": three_days_before_str,
                "status": "SUBMITTED_SUCCESS",
            },
            {
                "id_arvo": "p6",
                "submission_run_id": "run_B",
                "ingested_at": five_minutes_after_str,
                "status": "SUBMITTED_SUCCESS",
            },
            {
                "id_arvo": "p7",
                "submission_run_id": "run_B",
                "ingested_at": five_minutes_after_str,
                "status": "SUBMITTED_SUCCESS",
            },
            {
                "id_arvo": "u4",
                "submission_run_id": "run_B",
                "ingested_at": five_minutes_after_str,
                "status": "SUBMITTED_SUCCESS",
            },
        ]

        create_claims_table_with_data(
            bigquery_client, dataset_id, processable_table_id, rows=processable_rows
        )
        create_claims_table_with_data(
            bigquery_client, dataset_id, unprocessable_table_id, rows=unprocessable_rows
        )
        create_submitted_claims_table_with_data(
            bigquery_client, dataset_id, submitted_table_id, rows=submitted_rows
        )
        create_internal_validation_table_with_data(
            bigquery_client, dataset_id, internal_validation_table_id, rows=[]
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            partner="porto",
            processable_table_id=processable_table_id,
            unprocessable_table_id=unprocessable_table_id,
            internal_validation_table_id=internal_validation_table_id,
            submitted_table_id=submitted_table_id,
            submission_run_id=submission_run_id,
            source_timestamp=source_timestamp,
        )

        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="porto",
            values=[
                {
                    "status": "SUBMITTED_SUCCESS",
                    "perc_pago": 0.0,
                    "perc_info": 0.0,
                },
            ],
        )

        response = dispatch_event(event, [UnsentClaimsHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)
    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_handle_queries_and_emits_metrics_no_ingested_claims_to_submit(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Test that handle queries BigQuery and emits metrics for the sum of vl_pago and vl_info
    of unsent claims when there are no ingested claims to submit within the last 2 days.

    In this case, we should emit 100% for the success metric as there's nothing to submit.
    """

    dataset_id = "test_dataset"
    processable_table_id = "processable_claims"
    unprocessable_table_id = "unprocessable_claims"
    internal_validation_table_id = "internal_validation"
    submitted_table_id = "submitted_claims"
    submission_run_id = "run_X"
    source_timestamp = "2024-01-15T10:30:00Z"

    create_dataset(bigquery_client, dataset_id)

    try:
        # The fallback timestamp is the source_timestamp minus 20 minutes
        submitted_ingested_at = datetime.fromisoformat(source_timestamp) - timedelta(minutes=20)

        # Old ingested at (outside the range)
        three_days_before_str = (submitted_ingested_at - timedelta(days=3)).isoformat()
        # Recent ingested at (outside the range)
        five_minutes_after_str = (submitted_ingested_at + timedelta(minutes=5)).isoformat()

        processable_rows = [
            {
                "id_arvo": "p1",
                "vl_pago": 1000.0,
                "vl_info": 1000.0,
                "ingested_at": three_days_before_str,  # Ignored
            },
            {
                "id_arvo": "p6",
                "vl_pago": 1000.0,
                "vl_info": 1000.0,
                "ingested_at": five_minutes_after_str,  # Ignored
            },
            {
                "id_arvo": "p7",
                "vl_pago": 1000.0,
                "vl_info": 1000.0,
                "ingested_at": five_minutes_after_str,  # Ignored
            },
        ]

        unprocessable_rows = [
            {
                "id_arvo": "u1",
                "vl_pago": 1000.0,
                "vl_info": 1000.0,
                "ingested_at": three_days_before_str,  # Ignored
            },
            {
                "id_arvo": "u4",
                "vl_pago": 1000.0,
                "vl_info": 1000.0,
                "ingested_at": five_minutes_after_str,  # Ignored
            },
        ]

        submitted_rows = [
            {
                "id_arvo": "p1",
                "submission_run_id": "run_A",
                "ingested_at": three_days_before_str,
                "status": "SUBMITTED_SUCCESS",
            },
            {
                "id_arvo": "u1",
                "submission_run_id": "run_A",
                "ingested_at": three_days_before_str,
                "status": "SUBMITTED_SUCCESS",
            },
            {
                "id_arvo": "p6",
                "submission_run_id": "run_B",
                "ingested_at": five_minutes_after_str,
                "status": "SUBMITTED_SUCCESS",
            },
            {
                "id_arvo": "p7",
                "submission_run_id": "run_B",
                "ingested_at": five_minutes_after_str,
                "status": "SUBMITTED_SUCCESS",
            },
            {
                "id_arvo": "u4",
                "submission_run_id": "run_B",
                "ingested_at": five_minutes_after_str,
                "status": "SUBMITTED_SUCCESS",
            },
        ]

        create_claims_table_with_data(
            bigquery_client, dataset_id, processable_table_id, rows=processable_rows
        )
        create_claims_table_with_data(
            bigquery_client, dataset_id, unprocessable_table_id, rows=unprocessable_rows
        )
        create_submitted_claims_table_with_data(
            bigquery_client, dataset_id, submitted_table_id, rows=submitted_rows
        )
        create_internal_validation_table_with_data(
            bigquery_client, dataset_id, internal_validation_table_id, rows=[]
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            partner="porto",
            processable_table_id=processable_table_id,
            unprocessable_table_id=unprocessable_table_id,
            internal_validation_table_id=internal_validation_table_id,
            submitted_table_id=submitted_table_id,
            submission_run_id=submission_run_id,
            source_timestamp=source_timestamp,
        )

        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="porto",
            values=[
                {
                    "status": "SUBMITTED_SUCCESS",
                    "perc_pago": 1.0,
                    "perc_info": 1.0,
                },
            ],
        )

        response = dispatch_event(event, [UnsentClaimsHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)
    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
