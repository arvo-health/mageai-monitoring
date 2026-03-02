"""Unit and integration tests for SharedIdFaturaCompletenessHandler."""

from datetime import datetime, timedelta

import pytest
from cloudevents.http import CloudEvent
from google.cloud import bigquery
from pytest_mock import MockerFixture

from handlers.shared_id_fatura_completeness import SharedIdFaturaCompletenessHandler
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


def _create_expected_completeness_call(
    mocker: MockerFixture,
    partner: str,
    completeness: float,
) -> list:
    """Create expected metric call matcher for the completeness metric."""
    expected_project = "projects/arvo-eng-prd"

    return [
        mocker.call(
            name=expected_project,
            time_series=MetricMatcher(
                metric_type="claims/pipeline/savings/vl_glosa_arvo/shared_id_fatura_sent_over_validation",
                value=completeness,
                labels={"partner": partner},
            ),
        )
    ]


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
                    "pipeline_uuid": "pipesv2_selection",
                    "status": "COMPLETED",
                }
            },
            False,
        ),
        (
            {
                "payload": {
                    "pipeline_uuid": "pipesv2_submission",
                    "status": "FAILED",
                }
            },
            False,
        ),
        ({}, False),
    ],
)
def test_match(mocker: MockerFixture, decoded_message, expected):
    """Test that match returns the expected result for various message configurations."""
    handler = SharedIdFaturaCompletenessHandler(
        monitoring_client=mocker.MagicMock(),
        bq_client=mocker.MagicMock(),
        run_project_id="test-project",
        data_project_id="test-data-project",
    )

    assert handler.match(decoded_message) is expected


@pytest.mark.integration
def test_handle_all_shared_id_fatura_submitted_emits_one(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Test that when 100% of SHARED_ID_FATURA items are submitted, emits 1.0.

    Scenario:
    - internal_validation:
      * i1: APPROVED, SHARED_ID_FATURA, 600.0
      * i2: APPROVED, no reason, 400.0 (should NOT count in SHARED_ID_FATURA check)
    - manual_validation:
      * m1: SUBMITTED_SUCCESS, SHARED_ID_FATURA, 400.0
    - SHARED_ID_FATURA total = 600 + 400 = 1000
    - Submitted SHARED_ID_FATURA: i1 (600.0) + m1 (400.0) = 1000
    - Completeness = 1000 / 1000 = 1.0
    """
    dataset_id = "test_dataset_sifc_full"
    internal_table_id = "internal_validation"
    manual_table_id = "manual_validation"
    submitted_table_id = "submitted_claims"
    submission_run_id = "run_A"
    source_timestamp = "2024-01-15T10:30:00Z"

    create_dataset(bigquery_client, dataset_id)

    try:
        submitted_ingested_at = datetime.fromisoformat("2024-01-14T20:00:00Z")
        one_day_before_str = (submitted_ingested_at - timedelta(days=1)).isoformat()

        internal_rows = [
            {
                "id_arvo": "i1",
                "id_fatura": "f1",
                "vl_glosa_arvo": 600.0,
                "ingested_at": one_day_before_str,
                "status": "APPROVED",
                "filtered_reason": "SHARED_ID_FATURA",
            },
            {
                "id_arvo": "i2",
                "id_fatura": "f2",
                "vl_glosa_arvo": 400.0,
                "ingested_at": one_day_before_str,
                "status": "APPROVED",
                "filtered_reason": None,  # Not a SHARED_ID_FATURA item
            },
        ]

        manual_rows = [
            {
                "id_arvo": "m1",
                "id_fatura": "f3",
                "vl_glosa_arvo": 400.0,
                "ingested_at": one_day_before_str,
                "status": "SUBMITTED_SUCCESS",
                "filtered_reason": "SHARED_ID_FATURA",
            },
        ]

        submitted_rows = [
            {
                "id_arvo": "i1",
                "vl_glosa_arvo": 600.0,
                "ingested_at": one_day_before_str,
                "submission_run_id": submission_run_id,
                "status": "SUBMITTED_SUCCESS",
            },
            {
                "id_arvo": "m1",
                "vl_glosa_arvo": 400.0,
                "ingested_at": one_day_before_str,
                "submission_run_id": submission_run_id,
                "status": "SUBMITTED_SUCCESS",
            },
            {
                "id_arvo": "i2",
                "vl_glosa_arvo": 400.0,
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

        # SHARED_ID_FATURA total = 600 + 400 = 1000
        # All submitted → completeness = 1.0
        expected_calls = _create_expected_completeness_call(
            mocker, partner="petro", completeness=1.0
        )

        response = dispatch_event(event, [SharedIdFaturaCompletenessHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)
    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_handle_partial_shared_id_fatura_submitted_emits_partial(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Test that when only some SHARED_ID_FATURA items are submitted, emits < 1.0.

    Scenario:
    - internal_validation:
      * i1: APPROVED, SHARED_ID_FATURA, 600.0
      * i2: APPROVED, SHARED_ID_FATURA, 400.0
    - SHARED_ID_FATURA total = 1000
    - Only i1 (600.0) submitted → completeness = 600 / 1000 = 0.6
    """
    dataset_id = "test_dataset_sifc_partial"
    internal_table_id = "internal_validation"
    manual_table_id = "manual_validation"
    submitted_table_id = "submitted_claims"
    submission_run_id = "run_B"
    source_timestamp = "2024-01-15T10:30:00Z"

    create_dataset(bigquery_client, dataset_id)

    try:
        submitted_ingested_at = datetime.fromisoformat("2024-01-14T20:00:00Z")
        one_day_before_str = (submitted_ingested_at - timedelta(days=1)).isoformat()

        internal_rows = [
            {
                "id_arvo": "i1",
                "id_fatura": "f1",
                "vl_glosa_arvo": 600.0,
                "ingested_at": one_day_before_str,
                "status": "APPROVED",
                "filtered_reason": "SHARED_ID_FATURA",
            },
            {
                "id_arvo": "i2",
                "id_fatura": "f2",
                "vl_glosa_arvo": 400.0,
                "ingested_at": one_day_before_str,
                "status": "APPROVED",
                "filtered_reason": "SHARED_ID_FATURA",
            },
        ]

        submitted_rows = [
            {
                "id_arvo": "i1",
                "vl_glosa_arvo": 600.0,
                "ingested_at": one_day_before_str,
                "submission_run_id": submission_run_id,
                "status": "SUBMITTED_SUCCESS",
            },
            # i2 was NOT submitted
        ]

        create_validation_table_with_data(
            bigquery_client, dataset_id, internal_table_id, rows=internal_rows
        )
        create_validation_table_with_data(bigquery_client, dataset_id, manual_table_id, rows=[])
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

        # Completeness = 600 / 1000 = 0.6
        expected_calls = _create_expected_completeness_call(
            mocker, partner="petro", completeness=0.6
        )

        response = dispatch_event(event, [SharedIdFaturaCompletenessHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)
    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_handle_not_sent_for_validation_items_are_in_denominator(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Test that NOT_SENT_FOR_VALIDATION SHARED_ID_FATURA items are counted in denominator.

    SHARED_ID_FATURA items may have NOT_SENT_FOR_VALIDATION status (filtered at selection
    time) but are still expected to be submitted. If they are not in submitted_claims,
    completeness must be < 1.0 and the alert should fire.

    Scenario:
    - internal_validation:
      * i1: NOT_SENT_FOR_VALIDATION, SHARED_ID_FATURA, 400.0 → in denominator (not submitted)
      * i2: SUBMITTED_SUCCESS, SHARED_ID_FATURA, 600.0 → in denominator (submitted)
    - SHARED_ID_FATURA total = 400 + 600 = 1000
    - Only i2 submitted → completeness = 600 / 1000 = 0.6
    """
    dataset_id = "test_dataset_sifc_not_sent"
    internal_table_id = "internal_validation"
    manual_table_id = "manual_validation"
    submitted_table_id = "submitted_claims"
    submission_run_id = "run_D"
    source_timestamp = "2024-01-15T10:30:00Z"

    create_dataset(bigquery_client, dataset_id)

    try:
        submitted_ingested_at = datetime.fromisoformat("2024-01-14T20:00:00Z")
        one_day_before_str = (submitted_ingested_at - timedelta(days=1)).isoformat()

        internal_rows = [
            {
                "id_arvo": "i1",
                "id_fatura": "f1",
                "vl_glosa_arvo": 400.0,
                "ingested_at": one_day_before_str,
                "status": "NOT_SENT_FOR_VALIDATION",  # In denominator — not submitted
                "filtered_reason": "SHARED_ID_FATURA",
            },
            {
                "id_arvo": "i2",
                "id_fatura": "f2",
                "vl_glosa_arvo": 600.0,
                "ingested_at": one_day_before_str,
                "status": "SUBMITTED_SUCCESS",  # In denominator — submitted
                "filtered_reason": "SHARED_ID_FATURA",
            },
        ]

        submitted_rows = [
            {
                "id_arvo": "i2",
                "vl_glosa_arvo": 600.0,
                "ingested_at": one_day_before_str,
                "submission_run_id": submission_run_id,
                "status": "SUBMITTED_SUCCESS",
            },
            # i1 was NOT submitted → completeness < 1.0
        ]

        create_validation_table_with_data(
            bigquery_client, dataset_id, internal_table_id, rows=internal_rows
        )
        create_validation_table_with_data(bigquery_client, dataset_id, manual_table_id, rows=[])
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

        # Denominator = 400 (i1 NOT_SENT_FOR_VALIDATION) + 600 (i2 SUBMITTED_SUCCESS) = 1000
        # Submitted = 600 (i2 only) → completeness = 0.6
        expected_calls = _create_expected_completeness_call(
            mocker, partner="petro", completeness=0.6
        )

        response = dispatch_event(event, [SharedIdFaturaCompletenessHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)
    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_handle_no_shared_id_fatura_items_emits_one(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Test that when there are no SHARED_ID_FATURA items, emits 1.0 (trivially complete)."""
    dataset_id = "test_dataset_sifc_none"
    internal_table_id = "internal_validation"
    manual_table_id = "manual_validation"
    submitted_table_id = "submitted_claims"
    submission_run_id = "run_C"
    source_timestamp = "2024-01-15T10:30:00Z"

    create_dataset(bigquery_client, dataset_id)

    try:
        submitted_ingested_at = datetime.fromisoformat("2024-01-14T20:00:00Z")
        one_day_before_str = (submitted_ingested_at - timedelta(days=1)).isoformat()

        # Only items without SHARED_ID_FATURA
        internal_rows = [
            {
                "id_arvo": "i1",
                "id_fatura": "f1",
                "vl_glosa_arvo": 500.0,
                "ingested_at": one_day_before_str,
                "status": "APPROVED",
                "filtered_reason": None,
            },
        ]

        submitted_rows = [
            {
                "id_arvo": "i1",
                "vl_glosa_arvo": 500.0,
                "ingested_at": one_day_before_str,
                "submission_run_id": submission_run_id,
                "status": "SUBMITTED_SUCCESS",
            },
        ]

        create_validation_table_with_data(
            bigquery_client, dataset_id, internal_table_id, rows=internal_rows
        )
        create_validation_table_with_data(bigquery_client, dataset_id, manual_table_id, rows=[])
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

        # No SHARED_ID_FATURA items → completeness = 1.0
        expected_calls = _create_expected_completeness_call(
            mocker, partner="petro", completeness=1.0
        )

        response = dispatch_event(event, [SharedIdFaturaCompletenessHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)
    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
