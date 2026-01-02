"""Integration tests for NewBeneficiariesBaseHandler."""

from datetime import datetime, timedelta

import pytest
from cloudevents.http import CloudEvent
from pytest_mock import MockerFixture

from handlers.new_beneficiaries_approval import NewBeneficiariesApprovalHandler
from handlers.new_beneficiaries_wrangling import NewBeneficiariesWranglingHandler
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
    """Create expected metric call matchers."""
    expected_project = "projects/arvo-eng-prd"
    expected_labels = {"partner": partner, "approved": approved}

    return [
        mocker.call(
            name=expected_project,
            time_series=MetricMatcher(
                metric_type="claims/pipeline/beneficiaries/new_pct_last_3_mo",
                value=value,
                labels=expected_labels,
            ),
        ),
    ]


@pytest.mark.integration
def test_new_beneficiaries_base_handler_with_approval_pipeline(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for NewBeneficiariesBaseHandler via approval pipeline.

    This test:
    1. Creates BigQuery tables with test beneficiary data
    2. Triggers the handler with a pipesv2_approval completion event
    3. Verifies metrics are emitted correctly
    """
    dataset_id = "test_dataset"
    batch_processable_table_id = "batch_processable"
    batch_unprocessable_table_id = "batch_unprocessable"
    historical_processable_table_id = "historical_processable"
    historical_unprocessable_table_id = "historical_unprocessable"

    create_dataset(bigquery_client, dataset_id)

    try:
        # Batch data: 3 beneficiaries in "hospital" category, 2 in "clinic" category
        # Beneficiary "beneficiary1" appears in both processable and unprocessable
        # (will be deduplicated by DISTINCT)
        batch_processable_rows = [
            {
                "id_arvo": "arvo1",
                "id_matricula": "beneficiary1",
                "categoria": "hospital",
                "created_at": "2024-04-15T10:00:00Z",
            },
            {
                "id_arvo": "arvo2",
                "id_matricula": "beneficiary2",
                "categoria": "hospital",
                "created_at": "2024-04-15T10:00:00Z",
            },
            {
                "id_arvo": "arvo3",
                "id_matricula": "beneficiary3",
                "categoria": "clinic",
                "created_at": "2024-04-15T10:00:00Z",
            },
        ]
        batch_unprocessable_rows = [
            {
                "id_arvo": "arvo4",
                "id_matricula": "beneficiary1",  # Duplicate beneficiary, different id_arvo
                "categoria": "hospital",
                "created_at": "2024-04-15T10:00:00Z",
            },
            {
                "id_arvo": "arvo5",
                "id_matricula": "beneficiary4",
                "categoria": "clinic",
                "created_at": "2024-04-15T10:00:00Z",
            },
        ]

        # Historical data: beneficiary1 and beneficiary2 exist in hospital,
        # beneficiary3 exists in clinic
        # beneficiary4 is new (not in historical)
        # beneficiary5 is old (outside 3-month window)
        three_months_ago = datetime.fromisoformat("2024-04-15T10:30:00Z") - timedelta(days=90)
        historical_processable_rows = [
            {
                "id_arvo": "arvo6",
                "id_matricula": "beneficiary1",
                "categoria": "hospital",
                "created_at": three_months_ago.isoformat(),
            },
            {
                "id_arvo": "arvo7",
                "id_matricula": "beneficiary2",
                "categoria": "hospital",
                "created_at": three_months_ago.isoformat(),
            },
            {
                "id_arvo": "arvo8",
                "id_matricula": "beneficiary3",
                "categoria": "clinic",
                "created_at": three_months_ago.isoformat(),
            },
        ]
        historical_unprocessable_rows = [
            {
                "id_arvo": "arvo9",
                "id_matricula": "beneficiary5",  # Outside 3-month window
                "categoria": "clinic",
                "created_at": (three_months_ago - timedelta(days=1)).isoformat(),
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
            source_timestamp="2024-04-15T10:30:00Z",
        )

        # Expected results:
        # Batch has beneficiary1, beneficiary2, beneficiary3, beneficiary4 (4 unique)
        # Historical has beneficiary1, beneficiary2, beneficiary3 (3 unique,
        # beneficiary5 is outside window)
        # New beneficiaries: beneficiary4 (1), total: 4, percentage: 0.25

        expected_calls = _create_expected_metric_calls(
            mocker, partner="porto", approved="true", value=0.25
        )

        response = dispatch_event(event, [NewBeneficiariesApprovalHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_new_beneficiaries_base_handler_with_wrangling_pipeline(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for NewBeneficiariesBaseHandler via wrangling pipeline.

    This test:
    1. Creates BigQuery tables with test beneficiary data
    2. Triggers the handler with a pipesv2_wrangling completion event
    3. Verifies metrics are emitted correctly
    """
    dataset_id = "test_dataset"
    batch_processable_table_id = "batch_processable"
    batch_unprocessable_table_id = "batch_unprocessable"
    historical_processable_table_id = "historical_processable"
    historical_unprocessable_table_id = "historical_unprocessable"

    create_dataset(bigquery_client, dataset_id)

    try:
        # Batch data: 2 beneficiaries in "hospital" category, both new
        batch_processable_rows = [
            {
                "id_arvo": "arvo1",
                "id_matricula": "beneficiary1",
                "categoria": "hospital",
                "created_at": "2024-04-15T10:00:00Z",
            },
            {
                "id_arvo": "arvo2",
                "id_matricula": "beneficiary2",
                "categoria": "hospital",
                "created_at": "2024-04-15T10:00:00Z",
            },
        ]
        batch_unprocessable_rows = []

        # Historical data: empty (no beneficiaries in last 3 months)
        historical_processable_rows = []
        historical_unprocessable_rows = []

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
            source_timestamp="2024-04-15T10:30:00Z",
        )

        # Expected results:
        # Batch has beneficiary1, beneficiary2 (2 unique)
        # Historical has 0 beneficiaries
        # New beneficiaries: beneficiary1, beneficiary2 (2), total: 2, percentage: 1.0

        expected_calls = _create_expected_metric_calls(
            mocker, partner="abertta", approved="false", value=1.0
        )

        response = dispatch_event(event, [NewBeneficiariesWranglingHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_new_beneficiaries_base_handler_missing_historical_tables(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for NewBeneficiariesBaseHandler when historical tables don't exist.

    This test:
    1. Creates only batch tables (historical tables don't exist)
    2. Triggers the handler with a pipesv2_approval completion event
    3. Verifies that all beneficiaries are assumed to be new (100%)
    """
    dataset_id = "test_dataset"
    batch_processable_table_id = "batch_processable"
    batch_unprocessable_table_id = "batch_unprocessable"
    historical_processable_table_id = "historical_processable"
    historical_unprocessable_table_id = "historical_unprocessable"

    create_dataset(bigquery_client, dataset_id)

    try:
        # Batch data: 2 beneficiaries in "hospital" category
        batch_processable_rows = [
            {
                "id_arvo": "arvo1",
                "id_matricula": "beneficiary1",
                "categoria": "hospital",
                "created_at": "2024-04-15T10:00:00Z",
            },
            {
                "id_arvo": "arvo2",
                "id_matricula": "beneficiary2",
                "categoria": "hospital",
                "created_at": "2024-04-15T10:00:00Z",
            },
        ]
        batch_unprocessable_rows = []

        create_beneficiary_table_with_data(
            bigquery_client, dataset_id, batch_processable_table_id, batch_processable_rows
        )
        create_beneficiary_table_with_data(
            bigquery_client, dataset_id, batch_unprocessable_table_id, batch_unprocessable_rows
        )
        # Don't create historical tables

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

        # Expected results:
        # Batch has beneficiary1, beneficiary2 (2 unique)
        # Historical tables don't exist, so assume 100% new
        # percentage: 1.0

        expected_calls = _create_expected_metric_calls(
            mocker, partner="cemig", approved="true", value=1.0
        )

        response = dispatch_event(event, [NewBeneficiariesApprovalHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_new_beneficiaries_base_handler_with_3month_window(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for NewBeneficiariesBaseHandler with 3-month window filtering.

    This test:
    1. Creates historical tables with beneficiaries both inside and outside 3-month window
    2. Triggers the handler
    3. Verifies that only beneficiaries within 3-month window are considered
    """
    dataset_id = "test_dataset"
    batch_processable_table_id = "batch_processable"
    batch_unprocessable_table_id = "batch_unprocessable"
    historical_processable_table_id = "historical_processable"
    historical_unprocessable_table_id = "historical_unprocessable"

    create_dataset(bigquery_client, dataset_id)

    try:
        # Batch data: beneficiary1 (exists in historical within window),
        # beneficiary2 (exists outside window)
        batch_processable_rows = [
            {
                "id_arvo": "arvo1",
                "id_matricula": "beneficiary1",
                "categoria": "hospital",
                "created_at": "2024-04-15T10:00:00Z",
            },
            {
                "id_arvo": "arvo2",
                "id_matricula": "beneficiary2",
                "categoria": "hospital",
                "created_at": "2024-04-15T10:00:00Z",
            },
        ]
        batch_unprocessable_rows = []

        # Historical data: beneficiary1 within window, beneficiary2 outside window
        source_timestamp = datetime.fromisoformat("2024-04-15T10:30:00Z")
        three_months_ago = source_timestamp - timedelta(days=90)
        within_window = three_months_ago.isoformat()
        outside_window = (three_months_ago - timedelta(days=1)).isoformat()

        historical_processable_rows = [
            {
                "id_arvo": "arvo3",
                "id_matricula": "beneficiary1",
                "categoria": "hospital",
                "created_at": within_window,
            },
            {
                "id_arvo": "arvo4",
                "id_matricula": "beneficiary2",
                "categoria": "hospital",
                "created_at": outside_window,  # Outside 3-month window
            },
        ]
        historical_unprocessable_rows = []

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
            partner="athena",
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

        # Expected results:
        # Batch has beneficiary1, beneficiary2 (2 unique)
        # Historical within window has beneficiary1 (1 unique, beneficiary2 is outside window)
        # New beneficiaries: beneficiary2 (1), total: 2, percentage: 0.5

        expected_calls = _create_expected_metric_calls(
            mocker, partner="athena", approved="true", value=0.5
        )

        response = dispatch_event(event, [NewBeneficiariesApprovalHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_new_beneficiaries_base_handler_batch_exclusion_simple(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for NewBeneficiariesBaseHandler batch exclusion.

    This test verifies that historical items with the same id_arvo as batch items
    are excluded from the historical lookup. This is a simpler version that tests
    the basic exclusion behavior.

    This test:
    1. Creates batch and historical tables where batch item shares id_arvo with historical
    2. Triggers the handler
    3. Verifies that batch item is correctly identified as new (because historical is excluded)
    """
    dataset_id = "test_dataset"
    batch_processable_table_id = "batch_processable"
    batch_unprocessable_table_id = "batch_unprocessable"
    historical_processable_table_id = "historical_processable"
    historical_unprocessable_table_id = "historical_unprocessable"

    create_dataset(bigquery_client, dataset_id)

    try:
        # Batch data: beneficiary1 with id_arvo=arvo1
        batch_processable_rows = [
            {
                "id_arvo": "arvo1",
                "id_matricula": "beneficiary1",
                "categoria": "hospital",
                "created_at": "2024-04-15T10:00:00Z",
            },
        ]
        batch_unprocessable_rows = []

        # Historical data: same id_arvo as batch (should be excluded from lookup)
        source_timestamp = datetime.fromisoformat("2024-04-15T10:30:00Z")
        three_months_ago = source_timestamp - timedelta(days=90)

        historical_processable_rows = [
            {
                "id_arvo": "arvo1",  # Same id_arvo as batch - should be excluded
                "id_matricula": "beneficiary2",  # Different beneficiary
                "categoria": "hospital",
                "created_at": three_months_ago.isoformat(),
            },
        ]
        historical_unprocessable_rows = []

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

        # Expected results:
        # Batch has beneficiary1 (1 unique) with id_arvo=arvo1
        # Historical has items with id_arvo=arvo1, but these are excluded from lookup
        # because id_arvo=arvo1 exists in batch
        # New beneficiaries: beneficiary1 (1), total: 1, percentage: 1.0

        expected_calls = _create_expected_metric_calls(
            mocker, partner="cemig", approved="true", value=1.0
        )

        response = dispatch_event(event, [NewBeneficiariesApprovalHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_new_beneficiaries_base_handler_excludes_batch_items_from_historical(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for NewBeneficiariesBaseHandler excluding batch items
    from historical lookup.

    This test verifies that historical items with the same id_arvo as batch items
    are excluded from the historical lookup, preventing false negatives where
    batch items would always match historical items.

    This test:
    1. Creates batch and historical tables where some items share id_arvo values
    2. Historical table has a beneficiary that would match batch beneficiary if not excluded
    3. Triggers the handler
    4. Verifies that batch items are correctly identified as new despite having
       matching id_arvo in historical (because historical items are excluded)
    """
    dataset_id = "test_dataset"
    batch_processable_table_id = "batch_processable"
    batch_unprocessable_table_id = "batch_unprocessable"
    historical_processable_table_id = "historical_processable"
    historical_unprocessable_table_id = "historical_unprocessable"

    create_dataset(bigquery_client, dataset_id)

    try:
        source_timestamp = datetime.fromisoformat("2024-04-15T10:30:00Z")
        three_months_ago = source_timestamp - timedelta(days=90)

        # Batch data: beneficiary1 with id_arvo=arvo1, beneficiary2 with id_arvo=arvo2
        batch_processable_rows = [
            {
                "id_arvo": "arvo1",
                "id_matricula": "beneficiary1",
                "categoria": "hospital",
                "created_at": "2024-04-15T10:00:00Z",
            },
            {
                "id_arvo": "arvo2",
                "id_matricula": "beneficiary2",
                "categoria": "hospital",
                "created_at": "2024-04-15T10:00:00Z",
            },
        ]
        batch_unprocessable_rows = []

        # Historical data:
        # - id_arvo=arvo1 with beneficiary1 (same as batch) - should be EXCLUDED
        # - id_arvo=arvo3 with beneficiary2 (same beneficiary, different id_arvo)
        #   - should be INCLUDED
        # - id_arvo=arvo4 with beneficiary3 (different beneficiary) - should be INCLUDED
        historical_processable_rows = [
            {
                "id_arvo": "arvo1",  # Same id_arvo as batch - should be excluded
                "id_matricula": "beneficiary1",  # Same beneficiary as batch
                "categoria": "hospital",
                "created_at": three_months_ago.isoformat(),
            },
            {
                "id_arvo": "arvo3",  # Different id_arvo from batch - should be included
                "id_matricula": "beneficiary2",  # Same beneficiary as batch, but different id_arvo
                "categoria": "hospital",
                "created_at": three_months_ago.isoformat(),
            },
            {
                "id_arvo": "arvo4",  # Different id_arvo from batch - should be included
                "id_matricula": "beneficiary3",  # Different beneficiary
                "categoria": "hospital",
                "created_at": three_months_ago.isoformat(),
            },
        ]
        historical_unprocessable_rows = []

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
            partner="test_partner",
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

        # Expected results:
        # Batch has beneficiary1 (id_arvo=arvo1), beneficiary2 (id_arvo=arvo2)
        # Historical lookup (after excluding id_arvo=arvo1) has:
        #   - beneficiary2 (id_arvo=arvo3) - matches batch beneficiary2
        #   - beneficiary3 (id_arvo=arvo4) - doesn't match
        # New beneficiaries: beneficiary1 (1) - beneficiary2 exists in historical
        # with different id_arvo
        # Total: 2, percentage: 0.5

        expected_calls = _create_expected_metric_calls(
            mocker, partner="test_partner", approved="true", value=0.5
        )

        response = dispatch_event(event, [NewBeneficiariesApprovalHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
