"""Unit and integration tests for ExpiredValidationClaimsHandler."""

from datetime import datetime, timedelta

import pytest
from cloudevents.http import CloudEvent
from google.cloud import bigquery
from pytest_mock import MockerFixture

from handlers.expired_validation_claims import ExpiredValidationClaimsHandler
from tests.bigquery import create_dataset
from tests.conftest import assert_response_success
from tests.metrics import MetricMatcher, assert_metrics_emitted


def _create_cloud_event(
    bigquery_client,
    dataset_id: str,
    partner: str,
    internal_validation_table_id: str,
    manual_validation_table_id: str,
    source_timestamp: str,
) -> CloudEvent:
    """Create a CloudEvent for pipeline completion."""
    variables = {
        "partner": partner,
        "internal_validation_claims_input_table": (
            f"{bigquery_client.project}.{dataset_id}.{internal_validation_table_id}"
        ),
        "manual_validation_claims_input_table": (
            f"{bigquery_client.project}.{dataset_id}.{manual_validation_table_id}"
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
                "pipeline_uuid": "pipesv2_release",
                "status": "COMPLETED",
                "variables": variables,
            },
        },
    )


def _create_expected_metric_calls(
    mocker: MockerFixture,
    partner: str,
    combined_value: float,
) -> list:
    """Create expected metric call matchers."""
    expected_project = "projects/arvo-eng-prd"

    return [
        mocker.call(
            name=expected_project,
            time_series=MetricMatcher(
                metric_type="claims/pipeline/validation/vl_glosa_arvo/expired/total",
                value=combined_value,
                labels={"partner": partner},
            ),
        ),
    ]


def create_expired_validation_table_with_data(
    bigquery_client, dataset_id: str, table_id: str, rows: list[dict]
) -> None:
    """Create an expired validation claims table with required columns.

    Inserts test data into the created table.
    """
    schema = [
        bigquery.SchemaField("id_arvo", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("id_fatura", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("vl_glosa_arvo", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("updated_at", "DATETIME", mode="NULLABLE"),
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
                    "pipeline_uuid": "pipesv2_release",
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
                    "pipeline_uuid": "pipesv2_release",
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
    handler = ExpiredValidationClaimsHandler(
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
    """Test that handle queries BigQuery and emits metrics for the sum of vl_glosa_arvo
    of expired claims from both internal and manual validation tables."""

    dataset_id = "test_dataset"
    internal_validation_table_id = "internal_validation_claims"
    manual_validation_table_id = "manual_validation_claims"
    source_timestamp = "2024-01-15T10:30:00Z"

    create_dataset(bigquery_client, dataset_id)

    try:
        source_ts = datetime.fromisoformat(source_timestamp.replace("Z", "+00:00"))

        # Within partition range (2 days) and updated_at range (1 hour)
        valid_ingested_at = (source_ts - timedelta(days=1)).isoformat()
        valid_updated_at = (source_ts - timedelta(minutes=30)).isoformat()

        # Outside partition range (more than 2 days ago)
        old_ingested_at = (source_ts - timedelta(days=3)).isoformat()

        # Outside updated_at range (more than 30 minutes ago)
        old_updated_at = (source_ts - timedelta(minutes=45)).isoformat()

        internal_validation_rows = [
            {
                "id_arvo": "i1",
                "id_fatura": "f1",
                "vl_glosa_arvo": 100.0,
                "ingested_at": valid_ingested_at,
                "updated_at": valid_updated_at,
                "status": "EXPIRED",
            },
            {
                "id_arvo": "i2",
                "id_fatura": "f1",
                "vl_glosa_arvo": 200.0,
                "ingested_at": valid_ingested_at,
                "updated_at": valid_updated_at,
                "status": "EXPIRED",
            },
            {
                "id_arvo": "i3",
                "id_fatura": "f2",
                "vl_glosa_arvo": 50.0,
                "ingested_at": valid_ingested_at,
                "updated_at": valid_updated_at,
                "status": "APPROVED",  # Not EXPIRED - should be ignored
            },
            {
                "id_arvo": "i4",
                "id_fatura": "f3",
                "vl_glosa_arvo": 1000.0,
                "ingested_at": old_ingested_at,  # Outside partition range
                "updated_at": valid_updated_at,
                "status": "EXPIRED",
            },
            {
                "id_arvo": "i5",
                "id_fatura": "f4",
                "vl_glosa_arvo": 500.0,
                "ingested_at": valid_ingested_at,
                "updated_at": old_updated_at,  # Outside updated_at range
                "status": "EXPIRED",
            },
        ]

        manual_validation_rows = [
            {
                "id_arvo": "m1",
                "id_fatura": "f5",
                "vl_glosa_arvo": 150.0,
                "ingested_at": valid_ingested_at,
                "updated_at": valid_updated_at,
                "status": "EXPIRED",
            },
            {
                "id_arvo": "m2",
                "id_fatura": "f5",
                "vl_glosa_arvo": 75.0,
                "ingested_at": valid_ingested_at,
                "updated_at": valid_updated_at,
                "status": "EXPIRED",
            },
            {
                "id_arvo": "m3",
                "id_fatura": "f6",
                "vl_glosa_arvo": 300.0,
                "ingested_at": valid_ingested_at,
                "updated_at": valid_updated_at,
                "status": "DENIED",  # Not EXPIRED - should be ignored
            },
        ]

        create_expired_validation_table_with_data(
            bigquery_client, dataset_id, internal_validation_table_id, rows=internal_validation_rows
        )
        create_expired_validation_table_with_data(
            bigquery_client, dataset_id, manual_validation_table_id, rows=manual_validation_rows
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            partner="cemig",
            internal_validation_table_id=internal_validation_table_id,
            manual_validation_table_id=manual_validation_table_id,
            source_timestamp=source_timestamp,
        )

        # Expected: internal = 100 + 200 = 300, manual = 150 + 75 = 225, combined = 525
        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="cemig",
            combined_value=525.0,
        )

        response = dispatch_event(event, [ExpiredValidationClaimsHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)
    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_handle_emits_zero_when_no_expired_claims(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Test that handle emits metrics with value 0 when there are no expired claims."""

    dataset_id = "test_dataset"
    internal_validation_table_id = "internal_validation_claims"
    manual_validation_table_id = "manual_validation_claims"
    source_timestamp = "2024-01-15T10:30:00Z"

    create_dataset(bigquery_client, dataset_id)

    try:
        source_ts = datetime.fromisoformat(source_timestamp.replace("Z", "+00:00"))

        valid_ingested_at = (source_ts - timedelta(days=1)).isoformat()
        valid_updated_at = (source_ts - timedelta(minutes=30)).isoformat()

        # No EXPIRED status - all should be ignored
        internal_validation_rows = [
            {
                "id_arvo": "i1",
                "id_fatura": "f1",
                "vl_glosa_arvo": 100.0,
                "ingested_at": valid_ingested_at,
                "updated_at": valid_updated_at,
                "status": "APPROVED",
            },
        ]

        manual_validation_rows = [
            {
                "id_arvo": "m1",
                "id_fatura": "f2",
                "vl_glosa_arvo": 200.0,
                "ingested_at": valid_ingested_at,
                "updated_at": valid_updated_at,
                "status": "DENIED",
            },
        ]

        create_expired_validation_table_with_data(
            bigquery_client, dataset_id, internal_validation_table_id, rows=internal_validation_rows
        )
        create_expired_validation_table_with_data(
            bigquery_client, dataset_id, manual_validation_table_id, rows=manual_validation_rows
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            partner="cemig",
            internal_validation_table_id=internal_validation_table_id,
            manual_validation_table_id=manual_validation_table_id,
            source_timestamp=source_timestamp,
        )

        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="cemig",
            combined_value=0.0,
        )

        response = dispatch_event(event, [ExpiredValidationClaimsHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)
    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_handle_emits_zero_when_tables_are_empty(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Test that handle emits metrics with value 0 when tables are empty."""

    dataset_id = "test_dataset"
    internal_validation_table_id = "internal_validation_claims"
    manual_validation_table_id = "manual_validation_claims"
    source_timestamp = "2024-01-15T10:30:00Z"

    create_dataset(bigquery_client, dataset_id)

    try:
        # Create empty tables
        create_expired_validation_table_with_data(
            bigquery_client, dataset_id, internal_validation_table_id, rows=[]
        )
        create_expired_validation_table_with_data(
            bigquery_client, dataset_id, manual_validation_table_id, rows=[]
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            partner="cemig",
            internal_validation_table_id=internal_validation_table_id,
            manual_validation_table_id=manual_validation_table_id,
            source_timestamp=source_timestamp,
        )

        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="cemig",
            combined_value=0.0,
        )

        response = dispatch_event(event, [ExpiredValidationClaimsHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)
    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
