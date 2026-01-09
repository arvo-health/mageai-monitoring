"""Unit and integration tests for UnsentSavingsHandler."""

from datetime import datetime, timedelta

import pytest
from cloudevents.http import CloudEvent
from google.cloud import bigquery
from pytest_mock import MockerFixture

from handlers.unsent_savings import UnsentSavingsHandler
from tests.bigquery import create_dataset
from tests.conftest import assert_response_success
from tests.metrics import MetricMatcher, assert_metrics_emitted


def _create_cloud_event(
    bigquery_client,
    dataset_id: str,
    partner: str,
    selected_savings_table_id: str,
    internal_validation_table_id: str,
    manual_validation_table_id: str,
    submitted_table_id: str,
    submission_run_id: str,
    source_timestamp: str,
) -> CloudEvent:
    """Create a CloudEvent for pipeline completion."""
    variables = {
        "partner": partner,
        "submission_run_id": submission_run_id,
        "selected_savings_historical_table": (
            f"{bigquery_client.project}.{dataset_id}.{selected_savings_table_id}"
        ),
        "internal_validation_output_table": (
            f"{bigquery_client.project}.{dataset_id}.{internal_validation_table_id}"
        ),
        "manual_validation_output_table": (
            f"{bigquery_client.project}.{dataset_id}.{manual_validation_table_id}"
        ),
        "claims_submitted_output_table": (
            f"{bigquery_client.project}.{dataset_id}.{submitted_table_id}"
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
                    metric_type="claims/pipeline/savings/vl_glosa_arvo/sent_over_accepted_last_2_days",
                    value=value["perc"],
                    labels={"partner": partner, "status": value["status"]},
                ),
            ),
        )

    return calls


def create_savings_table_with_data(
    bigquery_client, dataset_id: str, table_id: str, rows: list[dict]
) -> None:
    """Create a savings table with id_arvo, vl_glosa_arvo and ingested_at columns.

    Inserts test data into the created table.
    """
    schema = [
        bigquery.SchemaField("id_arvo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("vl_glosa_arvo", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="NULLABLE"),
    ]
    table = bigquery.Table(f"{bigquery_client.project}.{dataset_id}.{table_id}", schema=schema)
    bigquery_client.create_table(table, exists_ok=True)
    bigquery_client.insert_rows_json(f"{bigquery_client.project}.{dataset_id}.{table_id}", rows)


def create_validation_table_with_data(
    bigquery_client, dataset_id: str, table_id: str, rows: list[dict]
) -> None:
    """Create a validation table with id_arvo, vl_glosa_arvo, ingested_at, and status columns.

    Inserts test data into the created table.
    """
    schema = [
        bigquery.SchemaField("id_arvo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("vl_glosa_arvo", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("id_fatura", "STRING", mode="NULLABLE"),
    ]
    table = bigquery.Table(f"{bigquery_client.project}.{dataset_id}.{table_id}", schema=schema)
    bigquery_client.create_table(table, exists_ok=True)
    bigquery_client.insert_rows_json(f"{bigquery_client.project}.{dataset_id}.{table_id}", rows)


def create_submitted_savings_table_with_data(
    bigquery_client, dataset_id: str, table_id: str, rows: list[dict]
) -> None:
    """Create a submitted claims table with id_arvo, vl_glosa_arvo, ingested_at,
    submission_run_id, and status columns.

    Inserts test data into the created table.
    """
    schema = [
        bigquery.SchemaField("id_arvo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("vl_glosa_arvo", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("submission_run_id", "STRING", mode="NULLABLE"),
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
    handler = UnsentSavingsHandler(
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
    """Test that handle queries BigQuery and emits metrics for the percentage of vl_glosa_arvo
    of submitted savings over accepted savings."""

    dataset_id = "test_dataset"
    selected_savings_table_id = "selected_savings"
    internal_validation_table_id = "internal_validation"
    manual_validation_table_id = "manual_validation"
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
        (submitted_ingested_at + timedelta(minutes=5)).isoformat()

        # Accepted savings: selected_savings (1000), internal_validation APPROVED (500),
        # manual_validation SUBMITTED_SUCCESS (300) = 1800 total
        # Within range: selected (1000), internal APPROVED (500), manual SUCCESS (300) = 1800
        # Outside range: selected (2000 - ignored), internal REJECTED (100 - ignored),
        # manual REJECTED (50 - ignored)
        selected_savings_rows = [
            {
                "id_arvo": "s1",
                "vl_glosa_arvo": 1000.0,
                "ingested_at": one_day_before_str,  # Within range
            },
            {
                "id_arvo": "s2",
                "vl_glosa_arvo": 2000.0,
                "ingested_at": three_days_before_str,  # Outside range
            },
        ]

        internal_validation_rows = [
            {
                "id_arvo": "i1",
                "id_fatura": "f1",
                "vl_glosa_arvo": 500.0,
                "ingested_at": one_day_before_str,
                "status": "APPROVED",  # Accepted
            },
            {
                "id_arvo": "i2",
                "id_fatura": "f1",
                "vl_glosa_arvo": 100.0,
                "ingested_at": one_day_before_str,
                "status": "REJECTED",  # Not accepted
            },
            {
                "id_arvo": "i3",
                "id_fatura": "f1",
                "vl_glosa_arvo": 200.0,
                "ingested_at": submitted_ingested_at_str,
                "status": "SUBMITTED_SUCCESS",  # Accepted
            },
            {
                "id_arvo": "i4",
                "id_fatura": "f2",
                "vl_glosa_arvo": 100.0,
                "ingested_at": submitted_ingested_at_str,
                "status": "SENT_FOR_VALIDATION",  # Not accepted
            },
            {
                "id_arvo": "i5",
                "id_fatura": "f2",
                "vl_glosa_arvo": 100.0,
                "ingested_at": submitted_ingested_at_str,
                "status": "APPROVED",  # Not accepted (claim pending validation)
            },
        ]

        manual_validation_rows = [
            {
                "id_arvo": "m1",
                "vl_glosa_arvo": 300.0,
                "id_fatura": "f3",
                "ingested_at": one_day_before_str,
                "status": "SUBMITTED_SUCCESS",  # Accepted
            },
            {
                "id_arvo": "m2",
                "id_fatura": "f3",
                "vl_glosa_arvo": 50.0,
                "ingested_at": one_day_before_str,
                "status": "REJECTED",  # Not accepted
            },
            {
                "id_arvo": "m3",
                "id_fatura": "f4",
                "vl_glosa_arvo": 100.0,
                "ingested_at": submitted_ingested_at_str,
                "status": "SENT_FOR_VALIDATION",  # Not accepted
            },
            {
                "id_arvo": "m4",
                "id_fatura": "f4",
                "vl_glosa_arvo": 100.0,
                "ingested_at": submitted_ingested_at_str,
                "status": "APPROVED",  # Not accepted (claim pending validation)
            },
        ]

        # Submitted savings: SUBMISSION_SUCCESS (400), SUBMISSION_ERROR (200),
        # RETRY (100) = 700 total
        # Total accepted = 1000 + 500 + 200 + 300 = 2000
        # Percentages: SUCCESS = 400/2000 = 0.2, ERROR = 200/2000 = 0.1,
        # RETRY = 100/2000 = 0.05
        # Only rows with matching id_arvo from accepted_savings are included
        submitted_rows = [
            {
                "id_arvo": "s1",  # Matches selected_savings
                "vl_glosa_arvo": 400.0,
                "ingested_at": one_day_before_str,
                "submission_run_id": submission_run_id,
                "status": "SUBMISSION_SUCCESS",
            },
            {
                "id_arvo": "i1",  # Matches internal_validation
                "vl_glosa_arvo": 200.0,
                "ingested_at": one_day_before_str,
                "submission_run_id": submission_run_id,
                "status": "SUBMISSION_ERROR",
            },
            {
                "id_arvo": "i3",  # Matches internal_validation
                "vl_glosa_arvo": 100.0,
                "ingested_at": submitted_ingested_at_str,
                "submission_run_id": submission_run_id,
                "status": "RETRY",
            },
            {
                "id_arvo": "other",  # Not in accepted_savings, should be excluded
                "vl_glosa_arvo": 999.0,
                "ingested_at": one_day_before_str,
                "submission_run_id": submission_run_id,
                "status": "SUBMISSION_SUCCESS",
            },
        ]

        create_savings_table_with_data(
            bigquery_client, dataset_id, selected_savings_table_id, rows=selected_savings_rows
        )
        create_validation_table_with_data(
            bigquery_client, dataset_id, internal_validation_table_id, rows=internal_validation_rows
        )
        create_validation_table_with_data(
            bigquery_client, dataset_id, manual_validation_table_id, rows=manual_validation_rows
        )
        create_submitted_savings_table_with_data(
            bigquery_client, dataset_id, submitted_table_id, rows=submitted_rows
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            partner="porto",
            selected_savings_table_id=selected_savings_table_id,
            internal_validation_table_id=internal_validation_table_id,
            manual_validation_table_id=manual_validation_table_id,
            submitted_table_id=submitted_table_id,
            submission_run_id=submission_run_id,
            source_timestamp=source_timestamp,
        )

        # Total accepted = 1000 (selected) + 500 (internal APPROVED) +
        # 200 (internal SUCCESS) + 300 (manual SUCCESS) = 2000
        # Submitted: SUCCESS = 400, ERROR = 200, RETRY = 100
        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="porto",
            values=[
                {
                    "status": "SUBMISSION_SUCCESS",
                    "perc": 0.2,  # 400 / 2000
                },
                {
                    "status": "SUBMISSION_ERROR",
                    "perc": 0.1,  # 200 / 2000
                },
                {
                    "status": "RETRY",
                    "perc": 0.05,  # 100 / 2000
                },
            ],
        )

        response = dispatch_event(event, [UnsentSavingsHandler])

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
    """Test that handle queries BigQuery and emits metrics when there are no submitted claims
    for the submission_run_id, using the fallback timestamp. Submitted claims from other runs
    within the time window are included."""

    dataset_id = "test_dataset"
    selected_savings_table_id = "selected_savings"
    internal_validation_table_id = "internal_validation"
    manual_validation_table_id = "manual_validation"
    submitted_table_id = "submitted_claims"
    submission_run_id = "run_X"
    source_timestamp = "2024-01-15T10:30:00Z"

    create_dataset(bigquery_client, dataset_id)

    try:
        # The fallback timestamp is the source_timestamp minus 20 minutes
        fallback_timestamp = datetime.fromisoformat(source_timestamp) - timedelta(minutes=20)

        # Latest ingested at (within the range)
        fallback_timestamp.isoformat()
        # Valid ingested at (within the range)
        one_day_before_str = (fallback_timestamp - timedelta(days=1)).isoformat()
        # Old ingested at (outside the range)
        three_days_before_str = (fallback_timestamp - timedelta(days=3)).isoformat()

        selected_savings_rows = [
            {
                "id_arvo": "s1",
                "vl_glosa_arvo": 1000.0,
                "ingested_at": one_day_before_str,  # Within range
            },
        ]

        internal_validation_rows = [
            {
                "id_arvo": "i1",
                "vl_glosa_arvo": 500.0,
                "ingested_at": one_day_before_str,
                "status": "APPROVED",
            },
        ]

        manual_validation_rows = [
            {
                "id_arvo": "m1",
                "vl_glosa_arvo": 300.0,
                "ingested_at": one_day_before_str,
                "status": "SUBMITTED_SUCCESS",
            },
        ]

        # No submitted claims for submission_run_id, but there are for other runs
        # Only rows with matching id_arvo from accepted_savings are included
        submitted_rows = [
            {
                "id_arvo": "s1",  # Matches selected_savings
                "vl_glosa_arvo": 200.0,
                "ingested_at": one_day_before_str,  # Within range, different run
                "submission_run_id": "run_A",  # Different run
                "status": "SUBMISSION_SUCCESS",
            },
            {
                "id_arvo": "other",  # Not in accepted_savings, should be excluded
                "vl_glosa_arvo": 100.0,
                "ingested_at": three_days_before_str,  # Outside range
                "submission_run_id": "run_B",  # Different run
                "status": "SUBMISSION_SUCCESS",
            },
        ]

        create_savings_table_with_data(
            bigquery_client, dataset_id, selected_savings_table_id, rows=selected_savings_rows
        )
        create_validation_table_with_data(
            bigquery_client, dataset_id, internal_validation_table_id, rows=internal_validation_rows
        )
        create_validation_table_with_data(
            bigquery_client, dataset_id, manual_validation_table_id, rows=manual_validation_rows
        )
        create_submitted_savings_table_with_data(
            bigquery_client, dataset_id, submitted_table_id, rows=submitted_rows
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            partner="porto",
            selected_savings_table_id=selected_savings_table_id,
            internal_validation_table_id=internal_validation_table_id,
            manual_validation_table_id=manual_validation_table_id,
            submitted_table_id=submitted_table_id,
            submission_run_id=submission_run_id,
            source_timestamp=source_timestamp,
        )

        # Total accepted = 1000 + 500 + 300 = 1800
        # Submitted from run_A (within time window) = 200
        # Percentage = 200 / 1800 = 0.111...
        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="porto",
            values=[
                {
                    "status": "SUBMISSION_SUCCESS",
                    "perc": 200.0 / 1800.0,
                },
            ],
        )

        response = dispatch_event(event, [UnsentSavingsHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)
    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_handle_queries_and_emits_metrics_no_accepted_savings(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Test that handle emits 1.0 for SUBMITTED_SUCCESS when there are no accepted savings
    (denominator is 0)."""

    dataset_id = "test_dataset"
    selected_savings_table_id = "selected_savings"
    internal_validation_table_id = "internal_validation"
    manual_validation_table_id = "manual_validation"
    submitted_table_id = "submitted_claims"
    submission_run_id = "run_X"
    source_timestamp = "2024-01-15T10:30:00Z"

    create_dataset(bigquery_client, dataset_id)

    try:
        # The fallback timestamp is the source_timestamp minus 20 minutes
        fallback_timestamp = datetime.fromisoformat(source_timestamp) - timedelta(minutes=20)

        # Old ingested at (outside the range)
        three_days_before_str = (fallback_timestamp - timedelta(days=3)).isoformat()
        # Recent ingested at (outside the range)
        (fallback_timestamp + timedelta(minutes=5)).isoformat()

        # All savings are outside the time window
        selected_savings_rows = [
            {
                "id_arvo": "s1",
                "vl_glosa_arvo": 1000.0,
                "ingested_at": three_days_before_str,  # Outside range
            },
        ]

        internal_validation_rows = [
            {
                "id_arvo": "i1",
                "vl_glosa_arvo": 500.0,
                "ingested_at": three_days_before_str,
                "status": "APPROVED",
            },
        ]

        manual_validation_rows = [
            {
                "id_arvo": "m1",
                "vl_glosa_arvo": 300.0,
                "ingested_at": three_days_before_str,
                "status": "SUBMITTED_SUCCESS",
            },
        ]

        # No submitted rows for this submission_run_id to trigger fallback
        # Since all accepted savings are outside the time window, no submitted rows will match
        submitted_rows = [
            {
                "id_arvo": "s1",  # Matches selected_savings but it's outside time window
                "vl_glosa_arvo": 100.0,
                "ingested_at": three_days_before_str,
                "submission_run_id": "run_OTHER",  # Different run_id to trigger fallback
                "status": "SUBMISSION_SUCCESS",
            },
        ]

        create_savings_table_with_data(
            bigquery_client, dataset_id, selected_savings_table_id, rows=selected_savings_rows
        )
        create_validation_table_with_data(
            bigquery_client, dataset_id, internal_validation_table_id, rows=internal_validation_rows
        )
        create_validation_table_with_data(
            bigquery_client, dataset_id, manual_validation_table_id, rows=manual_validation_rows
        )
        create_submitted_savings_table_with_data(
            bigquery_client, dataset_id, submitted_table_id, rows=submitted_rows
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            partner="porto",
            selected_savings_table_id=selected_savings_table_id,
            internal_validation_table_id=internal_validation_table_id,
            manual_validation_table_id=manual_validation_table_id,
            submitted_table_id=submitted_table_id,
            submission_run_id=submission_run_id,
            source_timestamp=source_timestamp,
        )

        # No accepted savings in time window, so emit 1.0 for SUBMITTED_SUCCESS
        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="porto",
            values=[
                {
                    "status": "SUBMITTED_SUCCESS",
                    "perc": 1.0,
                },
            ],
        )

        response = dispatch_event(event, [UnsentSavingsHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)
    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
