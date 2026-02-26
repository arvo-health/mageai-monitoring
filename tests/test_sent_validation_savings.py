"""Unit and integration tests for SentValidationSavingsHandler."""

from datetime import datetime, timedelta

import pytest
from cloudevents.http import CloudEvent
from google.cloud import bigquery
from pytest_mock import MockerFixture

from handlers.sent_validation_savings import SentValidationSavingsHandler
from tests.bigquery import create_dataset
from tests.conftest import assert_response_success
from tests.metrics import MetricMatcher, assert_metrics_emitted


def _create_cloud_event(
    bigquery_client,
    dataset_id: str,
    partner: str,
    internal_validation_table_id: str,
    manual_validation_table_id: str,
    submitted_table_id: str,
    submission_run_id: str,
    source_timestamp: str,
) -> CloudEvent:
    """Create a CloudEvent for pipesv2_submission pipeline completion."""
    variables = {
        "partner": partner,
        "submission_run_id": submission_run_id,
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
                    metric_type="claims/pipeline/savings/vl_glosa_arvo/sent_over_validation_last_2_days",
                    value=value["perc"],
                    labels={"partner": partner, "status": value["status"]},
                ),
            ),
        )

    return calls


def create_validation_table_with_data(
    bigquery_client, dataset_id: str, table_id: str, rows: list[dict]
) -> None:
    """Create a validation table with relevant columns and insert test data."""
    schema = [
        bigquery.SchemaField("id_arvo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("vl_glosa_arvo", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("id_fatura", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("filtered_reason", "STRING", mode="NULLABLE"),
    ]
    table = bigquery.Table(f"{bigquery_client.project}.{dataset_id}.{table_id}", schema=schema)
    bigquery_client.create_table(table, exists_ok=True)
    bigquery_client.insert_rows_json(f"{bigquery_client.project}.{dataset_id}.{table_id}", rows)


def create_submitted_table_with_data(
    bigquery_client, dataset_id: str, table_id: str, rows: list[dict]
) -> None:
    """Create a submitted claims table and insert test data."""
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
    handler = SentValidationSavingsHandler(
        monitoring_client=mocker.MagicMock(),
        bq_client=mocker.MagicMock(),
        run_project_id="test-project",
        data_project_id="test-data-project",
    )

    assert handler.match(decoded_message) is expected


@pytest.mark.integration
def test_handle_excludes_shared_id_fatura_and_emits_metrics(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Test that SHARED_ID_FATURA items are excluded and the metric is emitted correctly.

    Scenario:
    - internal_validation has 3 items:
      * i1: APPROVED, no filtered_reason, 500.0 → included in denominator
      * i2: APPROVED, filtered_reason=SHARED_ID_FATURA, 300.0 → excluded
      * i3: APPROVED, no filtered_reason, 200.0 → included in denominator
    - manual_validation has 1 item:
      * m1: SUBMITTED_SUCCESS, no filtered_reason, 400.0 → included in denominator
    - Denominator = 500 + 200 + 400 = 1100
    - submitted: i1 (400.0 SUBMITTED_SUCCESS), i3 (100.0 RETRY)
    - Percentages: SUCCESS = 400/1100, RETRY = 100/1100
    """

    dataset_id = "test_dataset_sv"
    internal_table_id = "internal_validation"
    manual_table_id = "manual_validation"
    submitted_table_id = "submitted_claims"
    submission_run_id = "run_X"
    source_timestamp = "2024-01-15T10:30:00Z"

    create_dataset(bigquery_client, dataset_id)

    try:
        submitted_ingested_at = datetime.fromisoformat("2024-01-14T20:00:00Z")
        submitted_ingested_at_str = submitted_ingested_at.isoformat()
        one_day_before_str = (submitted_ingested_at - timedelta(days=1)).isoformat()

        internal_rows = [
            {
                "id_arvo": "i1",
                "id_fatura": "f1",
                "vl_glosa_arvo": 500.0,
                "ingested_at": one_day_before_str,
                "status": "APPROVED",
                "filtered_reason": None,  # Included
            },
            {
                "id_arvo": "i2",
                "id_fatura": "f2",
                "vl_glosa_arvo": 300.0,
                "ingested_at": one_day_before_str,
                "status": "APPROVED",
                "filtered_reason": "SHARED_ID_FATURA",  # Excluded
            },
            {
                "id_arvo": "i3",
                "id_fatura": "f3",
                "vl_glosa_arvo": 200.0,
                "ingested_at": submitted_ingested_at_str,
                "status": "APPROVED",
                "filtered_reason": None,  # Included
            },
        ]

        manual_rows = [
            {
                "id_arvo": "m1",
                "id_fatura": "f4",
                "vl_glosa_arvo": 400.0,
                "ingested_at": one_day_before_str,
                "status": "SUBMITTED_SUCCESS",
                "filtered_reason": None,  # Included
            },
        ]

        submitted_rows = [
            {
                "id_arvo": "i1",
                "vl_glosa_arvo": 400.0,
                "ingested_at": one_day_before_str,
                "submission_run_id": submission_run_id,
                "status": "SUBMITTED_SUCCESS",
            },
            {
                "id_arvo": "i3",
                "vl_glosa_arvo": 100.0,
                "ingested_at": submitted_ingested_at_str,
                "submission_run_id": submission_run_id,
                "status": "RETRY",
            },
            {
                "id_arvo": "i2",  # SHARED_ID_FATURA item — should not be counted in this metric
                "vl_glosa_arvo": 300.0,
                "ingested_at": one_day_before_str,
                "submission_run_id": submission_run_id,
                "status": "SUBMITTED_SUCCESS",
            },
        ]

        create_validation_table_with_data(
            bigquery_client, dataset_id, internal_table_id, rows=internal_rows
        )
        create_validation_table_with_data(
            bigquery_client, dataset_id, manual_table_id, rows=manual_rows
        )
        create_submitted_table_with_data(
            bigquery_client, dataset_id, submitted_table_id, rows=submitted_rows
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            partner="petro",
            internal_validation_table_id=internal_table_id,
            manual_validation_table_id=manual_table_id,
            submitted_table_id=submitted_table_id,
            submission_run_id=submission_run_id,
            source_timestamp=source_timestamp,
        )

        # Denominator = 500 (i1) + 200 (i3) + 400 (m1) = 1100
        # SUBMITTED_SUCCESS = 400 / 1100
        # RETRY = 100 / 1100
        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="petro",
            values=[
                {"status": "SUBMITTED_SUCCESS", "perc": 400.0 / 1100.0},
                {"status": "RETRY", "perc": 100.0 / 1100.0},
            ],
        )

        response = dispatch_event(event, [SentValidationSavingsHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)
    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_handle_includes_approved_items_even_when_invoice_has_pending(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Test that APPROVED items are included even if their invoice has a SENT_FOR_VALIDATION item.

    Items stuck in SENT_FOR_VALIDATION should NOT cause other APPROVED items in
    the same invoice to be excluded from the denominator. The metric must account
    for all APPROVED savings that entered the validation pipeline.

    Scenario:
    - internal_validation:
      * i1: APPROVED, invoice f1, 500.0 → included
      * i2: APPROVED, invoice f2, 200.0 → included (same invoice as i3)
      * i3: SENT_FOR_VALIDATION, invoice f2, 100.0 → not counted (wrong status),
            but does NOT exclude i2
    - Denominator = 500 + 200 = 700
    - Submitted: i1 (500.0 SUBMITTED_SUCCESS)
    - SUBMITTED_SUCCESS = 500/700, SUBMITTED_SUCCESS(i2 not submitted) counted as 0
    """
    dataset_id = "test_dataset_sv_pending"
    internal_table_id = "internal_validation"
    manual_table_id = "manual_validation"
    submitted_table_id = "submitted_claims"
    submission_run_id = "run_Y"
    source_timestamp = "2024-01-15T10:30:00Z"

    create_dataset(bigquery_client, dataset_id)

    try:
        submitted_ingested_at = datetime.fromisoformat("2024-01-14T20:00:00Z")
        one_day_before_str = (submitted_ingested_at - timedelta(days=1)).isoformat()

        internal_rows = [
            {
                "id_arvo": "i1",
                "id_fatura": "f1",
                "vl_glosa_arvo": 500.0,
                "ingested_at": one_day_before_str,
                "status": "APPROVED",
                "filtered_reason": None,
            },
            {
                "id_arvo": "i2",
                "id_fatura": "f2",
                "vl_glosa_arvo": 200.0,
                "ingested_at": one_day_before_str,
                "status": "APPROVED",
                "filtered_reason": None,
            },
            {
                "id_arvo": "i3",
                "id_fatura": "f2",
                "vl_glosa_arvo": 100.0,
                "ingested_at": one_day_before_str,
                "status": "SENT_FOR_VALIDATION",  # Not counted, but does NOT exclude i2
                "filtered_reason": None,
            },
        ]

        manual_rows = []

        submitted_rows = [
            {
                "id_arvo": "i1",
                "vl_glosa_arvo": 500.0,
                "ingested_at": one_day_before_str,
                "submission_run_id": submission_run_id,
                "status": "SUBMITTED_SUCCESS",
            },
            # i2 was NOT submitted — it counts as 0 in the numerator
        ]

        create_validation_table_with_data(
            bigquery_client, dataset_id, internal_table_id, rows=internal_rows
        )
        create_validation_table_with_data(
            bigquery_client, dataset_id, manual_table_id, rows=manual_rows
        )
        create_submitted_table_with_data(
            bigquery_client, dataset_id, submitted_table_id, rows=submitted_rows
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            partner="petro",
            internal_validation_table_id=internal_table_id,
            manual_validation_table_id=manual_table_id,
            submitted_table_id=submitted_table_id,
            submission_run_id=submission_run_id,
            source_timestamp=source_timestamp,
        )

        # Denominator = 500 (i1) + 200 (i2) = 700
        # i2 is APPROVED in an invoice that has a pending item, but is still counted
        # SUBMITTED_SUCCESS = 500 / 700
        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="petro",
            values=[
                {"status": "SUBMITTED_SUCCESS", "perc": 500.0 / 700.0},
            ],
        )

        response = dispatch_event(event, [SentValidationSavingsHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)
    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_handle_no_validation_savings_emits_one(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Test that when there are no eligible validation savings, emits 1.0 for SUBMITTED_SUCCESS."""
    dataset_id = "test_dataset_sv_empty"
    internal_table_id = "internal_validation"
    manual_table_id = "manual_validation"
    submitted_table_id = "submitted_claims"
    submission_run_id = "run_Z"
    source_timestamp = "2024-01-15T10:30:00Z"

    create_dataset(bigquery_client, dataset_id)

    try:
        three_days_before_str = (
            datetime.fromisoformat("2024-01-14T20:00:00Z") - timedelta(days=3)
        ).isoformat()

        # All items are outside the time window
        internal_rows = [
            {
                "id_arvo": "i1",
                "id_fatura": "f1",
                "vl_glosa_arvo": 500.0,
                "ingested_at": three_days_before_str,  # Outside range
                "status": "APPROVED",
                "filtered_reason": None,
            },
        ]

        create_validation_table_with_data(
            bigquery_client, dataset_id, internal_table_id, rows=internal_rows
        )
        create_validation_table_with_data(
            bigquery_client, dataset_id, manual_table_id, rows=[]
        )
        create_submitted_table_with_data(
            bigquery_client, dataset_id, submitted_table_id, rows=[]
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            partner="petro",
            internal_validation_table_id=internal_table_id,
            manual_validation_table_id=manual_table_id,
            submitted_table_id=submitted_table_id,
            submission_run_id=submission_run_id,
            source_timestamp=source_timestamp,
        )

        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="petro",
            values=[
                {"status": "SUBMITTED_SUCCESS", "perc": 1.0},
            ],
        )

        response = dispatch_event(event, [SentValidationSavingsHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)
    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
