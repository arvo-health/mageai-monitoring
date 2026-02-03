"""Integration tests for BeneficiariesVolumeRatioBaseHandler."""

from datetime import datetime, timedelta

import pytest
from cloudevents.http import CloudEvent
from pytest_mock import MockerFixture

from handlers.beneficiaries_volume_ratio_approval import (
    BeneficiariesVolumeRatioApprovalHandler,
)
from handlers.beneficiaries_volume_ratio_wrangling import (
    BeneficiariesVolumeRatioWranglingHandler,
)
from tests.bigquery import create_beneficiary_table_with_data, create_dataset
from tests.conftest import assert_response_success
from tests.metrics import MetricMatcher, assert_metrics_emitted


def _create_cloud_event(
    bigquery_client,
    dataset_id: str,
    pipeline_uuid: str,
    partner: str,
    batch_processable_table_id: str,
    batch_unprocessable_table_id: str,
    historical_processable_table_id: str,
    historical_unprocessable_table_id: str,
    batch_processable_table_var: str,
    batch_unprocessable_table_var: str,
    historical_processable_table_var: str,
    historical_unprocessable_table_var: str,
    source_timestamp: str = "2024-04-15T10:30:00Z",
) -> CloudEvent:
    """Create a CloudEvent for pipeline completion."""
    variables = {
        "partner": partner,
        batch_processable_table_var: (
            f"{bigquery_client.project}.{dataset_id}.{batch_processable_table_id}"
        ),
        batch_unprocessable_table_var: (
            f"{bigquery_client.project}.{dataset_id}.{batch_unprocessable_table_id}"
        ),
        historical_processable_table_var: (
            f"{bigquery_client.project}.{dataset_id}.{historical_processable_table_id}"
        ),
        historical_unprocessable_table_var: (
            f"{bigquery_client.project}.{dataset_id}.{historical_unprocessable_table_id}"
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
    value: float,
) -> list:
    """Create expected metric call matchers for volume ratio."""
    expected_project = "projects/arvo-eng-prd"
    expected_labels = {"partner": partner, "approved": approved}

    return [
        mocker.call(
            name=expected_project,
            time_series=MetricMatcher(
                metric_type="claims/pipeline/beneficiaries/volume_ratio_last_1_mo",
                value=value,
                labels=expected_labels,
            ),
        ),
    ]


@pytest.mark.integration
def test_beneficiaries_volume_ratio_base_handler_approval(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for BeneficiariesVolumeRatioBaseHandler via approval pipeline.

    This test:
    1. Creates BigQuery tables with test beneficiary data distributed over time
    2. Triggers the handler with a pipesv2_approval completion event
    3. Verifies volume ratio metrics are emitted correctly
    """
    dataset_id = "test_dataset_vol_approval"
    batch_processable_table_id = "batch_processable"
    batch_unprocessable_table_id = "batch_unprocessable"
    historical_processable_table_id = "historical_processable"
    historical_unprocessable_table_id = "historical_unprocessable"

    create_dataset(bigquery_client, dataset_id)

    try:
        source_ts_str = "2024-04-15T10:30:00Z"
        source_timestamp = datetime.fromisoformat(source_ts_str)

        # Date ranges:
        # Latest (last 30 days): [2024-03-16, 2024-04-15]
        # Previous (D-60 to D-30): [2024-02-15, 2024-03-16)

        latest_date = (source_timestamp - timedelta(days=5)).isoformat()
        previous_date = (source_timestamp - timedelta(days=40)).isoformat()
        ignored_date = (source_timestamp - timedelta(days=61)).isoformat()

        # Batch data: (Assuming these are current/recent)
        # 2 beneficiaries (ben1, ben2)
        batch_processable_rows = [
            {
                "id_arvo": "arvo1",
                "id_matricula": "ben1",
                "categoria": "hospital",
                "created_at": latest_date,
            },
            {
                "id_arvo": "arvo2",
                "id_matricula": "ben2",
                "categoria": "hospital",
                "created_at": latest_date,
            },
        ]
        batch_unprocessable_rows = []

        # Historical data:
        # Latest period (last 30 days):
        # - ben3 (processable)
        # Previous period (30-60 days ago):
        # - ben4, ben5 (processable)
        # - ben6 (unprocessable)

        historical_processable_rows = [
            # In Latest Period
            {
                "id_arvo": "arvo3",
                "id_matricula": "ben3",
                "categoria": "hospital",
                "created_at": latest_date,
            },
            # In Previous Period
            {
                "id_arvo": "arvo4",
                "id_matricula": "ben4",
                "categoria": "hospital",
                "created_at": previous_date,
            },
            {
                "id_arvo": "arvo5",
                "id_matricula": "ben5",
                "categoria": "hospital",
                "created_at": previous_date,
            },
            # In Ignored Period
            {
                "id_arvo": "arvo6",
                "id_matricula": "ben6",
                "categoria": "hospital",
                "created_at": ignored_date,
            },
        ]

        historical_unprocessable_rows = [
            # In Previous Period
            {
                "id_arvo": "arvo7",
                "id_matricula": "ben7",
                "categoria": "clinic",
                "created_at": previous_date,
            },
            {
                "id_arvo": "arvo8",
                "id_matricula": "ben8",
                "categoria": "clinic",
                "created_at": previous_date,
            },
            # In Ignored Period
            {
                "id_arvo": "arvo9",
                "id_matricula": "ben9",
                "categoria": "clinic",
                "created_at": ignored_date,
            },
        ]

        create_beneficiary_table_with_data(
            bigquery_client, dataset_id, batch_processable_table_id, batch_processable_rows
        )
        create_beneficiary_table_with_data(
            bigquery_client, dataset_id, batch_unprocessable_table_id, batch_unprocessable_rows
        )
        create_beneficiary_table_with_data(
            bigquery_client,
            dataset_id,
            historical_processable_table_id,
            historical_processable_rows,
        )
        create_beneficiary_table_with_data(
            bigquery_client,
            dataset_id,
            historical_unprocessable_table_id,
            historical_unprocessable_rows,
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            pipeline_uuid="pipesv2_approval",
            partner="porto",
            batch_processable_table_id=batch_processable_table_id,
            batch_unprocessable_table_id=batch_unprocessable_table_id,
            historical_processable_table_id=historical_processable_table_id,
            historical_unprocessable_table_id=historical_unprocessable_table_id,
            batch_processable_table_var="processable_claims_input_table",
            batch_unprocessable_table_var="unprocessable_claims_input_table",
            historical_processable_table_var="processable_claims_output_table",
            historical_unprocessable_table_var="unprocessable_claims_output_table",
            source_timestamp=source_ts_str,
        )

        # Expected Calculation:
        # Latest Count = Batch (ben1, ben2) + Historical Latest (ben3) = 3
        # Previous Count = Historical Previous (ben4, ben5, ben7, ben8) = 4
        # Ratio = 3 / 4 = 0.75

        expected_calls = _create_expected_metric_calls(
            mocker, partner="porto", approved="true", value=0.75
        )

        response = dispatch_event(event, [BeneficiariesVolumeRatioApprovalHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_beneficiaries_volume_ratio_base_handler_wrangling(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for BeneficiariesVolumeRatioBaseHandler via wrangling pipeline."""
    dataset_id = "test_dataset_vol_wrangling"
    batch_processable_table_id = "batch_processable"
    batch_unprocessable_table_id = "batch_unprocessable"
    historical_processable_table_id = "historical_processable"
    historical_unprocessable_table_id = "historical_unprocessable"

    create_dataset(bigquery_client, dataset_id)

    try:
        source_ts_str = "2024-04-15T10:30:00Z"
        source_timestamp = datetime.fromisoformat(source_ts_str)

        latest_date = (source_timestamp - timedelta(days=5)).isoformat()
        previous_date = (source_timestamp - timedelta(days=40)).isoformat()

        # Batch: 1 beneficiary
        # Latest Historical: 0
        # Previous Historical: 2 beneficiaries

        batch_processable_rows = [
            {
                "id_arvo": "arvo1",
                "id_matricula": "ben1",
                "categoria": "hospital",
                "created_at": latest_date,
            },
        ]

        historical_processable_rows = [
            {
                "id_arvo": "arvo2",
                "id_matricula": "ben2",
                "categoria": "hospital",
                "created_at": previous_date,
            },
            {
                "id_arvo": "arvo3",
                "id_matricula": "ben3",
                "categoria": "hospital",
                "created_at": previous_date,
            },
        ]

        create_beneficiary_table_with_data(
            bigquery_client, dataset_id, batch_processable_table_id, batch_processable_rows
        )
        create_beneficiary_table_with_data(
            bigquery_client, dataset_id, batch_unprocessable_table_id, []
        )
        create_beneficiary_table_with_data(
            bigquery_client,
            dataset_id,
            historical_processable_table_id,
            historical_processable_rows,
        )
        create_beneficiary_table_with_data(
            bigquery_client, dataset_id, historical_unprocessable_table_id, []
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            pipeline_uuid="pipesv2_wrangling",
            partner="abertta",
            batch_processable_table_id=batch_processable_table_id,
            batch_unprocessable_table_id=batch_unprocessable_table_id,
            historical_processable_table_id=historical_processable_table_id,
            historical_unprocessable_table_id=historical_unprocessable_table_id,
            batch_processable_table_var="refined_processable_claims_output_table",
            batch_unprocessable_table_var="refined_unprocessable_claims_output_table",
            historical_processable_table_var="refined_processable_claims_historical_table",
            historical_unprocessable_table_var="refined_unprocessable_claims_historical_table",
            source_timestamp=source_ts_str,
        )

        # Expected Calculation:
        # Latest Count = Batch (ben1) = 1
        # Previous Count = Historical Previous (ben2, ben3) = 2
        # Ratio = 1 / 2 = 0.5

        expected_calls = _create_expected_metric_calls(
            mocker, partner="abertta", approved="false", value=0.5
        )

        response = dispatch_event(event, [BeneficiariesVolumeRatioWranglingHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_beneficiaries_volume_ratio_missing_historical(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Test behavior when historical tables are missing (should default to 1.0)."""
    dataset_id = "test_dataset_vol_missing"
    batch_processable_table_id = "batch_processable"
    batch_unprocessable_table_id = "batch_unprocessable"
    # Logic requires vars to be present but tables might not exist
    historical_processable_table_id = "historical_processable"
    historical_unprocessable_table_id = "historical_unprocessable"

    create_dataset(bigquery_client, dataset_id)

    try:
        # Only create batch tables
        batch_processable_rows = [
            {
                "id_arvo": "arvo1",
                "id_matricula": "ben1",
                "categoria": "hospital",
                "created_at": "2024-04-15T10:00:00Z",
            },
        ]

        create_beneficiary_table_with_data(
            bigquery_client, dataset_id, batch_processable_table_id, batch_processable_rows
        )
        create_beneficiary_table_with_data(
            bigquery_client, dataset_id, batch_unprocessable_table_id, []
        )
        # Verify tables exist

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            pipeline_uuid="pipesv2_approval",
            partner="cemig",
            batch_processable_table_id=batch_processable_table_id,
            batch_unprocessable_table_id=batch_unprocessable_table_id,
            historical_processable_table_id=historical_processable_table_id,
            historical_unprocessable_table_id=historical_unprocessable_table_id,
            batch_processable_table_var="processable_claims_input_table",
            batch_unprocessable_table_var="unprocessable_claims_input_table",
            historical_processable_table_var="processable_claims_output_table",
            historical_unprocessable_table_var="unprocessable_claims_output_table",
            source_timestamp="2024-04-15T10:30:00Z",
        )

        expected_calls = _create_expected_metric_calls(
            mocker, partner="cemig", approved="true", value=1.0
        )

        response = dispatch_event(event, [BeneficiariesVolumeRatioApprovalHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_beneficiaries_volume_ratio_zero_previous(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Test behavior when previous count is zero (should default to 1.0)."""
    dataset_id = "test_dataset_vol_zero_prev"
    batch_processable_table_id = "batch_processable"
    batch_unprocessable_table_id = "batch_unprocessable"
    historical_processable_table_id = "historical_processable"
    historical_unprocessable_table_id = "historical_unprocessable"

    create_dataset(bigquery_client, dataset_id)

    try:
        source_ts_str = "2024-04-15T10:30:00Z"
        source_timestamp = datetime.fromisoformat(source_ts_str)
        latest_date = (source_timestamp - timedelta(days=5)).isoformat()

        # Batch: 1 ben
        # Historical: Empty

        batch_processable_rows = [
            {
                "id_arvo": "arvo1",
                "id_matricula": "ben1",
                "categoria": "hospital",
                "created_at": latest_date,
            },
        ]

        create_beneficiary_table_with_data(
            bigquery_client, dataset_id, batch_processable_table_id, batch_processable_rows
        )
        create_beneficiary_table_with_data(
            bigquery_client, dataset_id, batch_unprocessable_table_id, []
        )
        create_beneficiary_table_with_data(
            bigquery_client, dataset_id, historical_processable_table_id, []
        )
        create_beneficiary_table_with_data(
            bigquery_client, dataset_id, historical_unprocessable_table_id, []
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            pipeline_uuid="pipesv2_approval",
            partner="porto",
            batch_processable_table_id=batch_processable_table_id,
            batch_unprocessable_table_id=batch_unprocessable_table_id,
            historical_processable_table_id=historical_processable_table_id,
            historical_unprocessable_table_id=historical_unprocessable_table_id,
            batch_processable_table_var="processable_claims_input_table",
            batch_unprocessable_table_var="unprocessable_claims_input_table",
            historical_processable_table_var="processable_claims_output_table",
            historical_unprocessable_table_var="unprocessable_claims_output_table",
            source_timestamp=source_ts_str,
        )

        # Expected:
        # Latest = 1
        # Previous = 0
        # Ratio = 1.0 (handled in SQL CASE WHEN previous_count = 0 THEN 1.0)

        expected_calls = _create_expected_metric_calls(
            mocker, partner="porto", approved="true", value=1.0
        )

        response = dispatch_event(event, [BeneficiariesVolumeRatioApprovalHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
