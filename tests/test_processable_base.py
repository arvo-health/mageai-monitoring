"""Integration tests for ProcessableBaseHandler."""

import pytest
from cloudevents.http import CloudEvent
from pytest_mock import MockerFixture

from handlers.processable_approval import ProcessableApprovalHandler
from handlers.processable_wrangling import ProcessableWranglingHandler
from tests.bigquery import (
    create_claims_table_with_data,
    create_dataset,
    create_savings_tables,
)
from tests.conftest import assert_response_success
from tests.metrics import MetricMatcher, assert_metrics_emitted

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

RELATIVE_VALUE = PROCESSABLE_CLAIMS_TOTAL / (
    PROCESSABLE_CLAIMS_TOTAL + UNPROCESSABLE_CLAIMS_TOTAL
)  # 0.8


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
        unprocessable_table_var: f"{bigquery_client.project}.{dataset_id}.{unprocessable_table_id}",
        processable_table_var: f"{bigquery_client.project}.{dataset_id}.{processable_table_id}",
    }

    if include_savings_tables:
        excluded_table_id, savings_table_id = create_savings_tables(bigquery_client, dataset_id)
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
                metric_type="claims/pipeline/processable/vl_pago/total",
                value=total_value,
                labels=expected_labels,
            ),
        ),
        mocker.call(
            name=expected_project,
            time_series=MetricMatcher(
                metric_type="claims/pipeline/processable/vl_pago/relative",
                value=relative_value,
                labels=expected_labels,
            ),
        ),
    ]


def _create_expected_metric_calls_total_only(
    mocker: MockerFixture, partner: str, approved: str, total_value: float
) -> list:
    """Create expected metric call matchers for total metric only."""
    expected_project = "projects/arvo-eng-prd"
    expected_labels = {"partner": partner, "approved": approved}

    return [
        mocker.call(
            name=expected_project,
            time_series=MetricMatcher(
                metric_type="claims/pipeline/processable/vl_pago/total",
                value=total_value,
                labels=expected_labels,
            ),
        ),
    ]


@pytest.mark.integration
def test_processable_base_handler_with_approval_pipeline(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for ProcessableBaseHandler via approval pipeline.

    This test:
    1. Creates BigQuery tables with test data
    2. Triggers the handler with a pipesv2_approval completion event
    3. Verifies both total and relative metrics are emitted correctly
    """
    dataset_id = "test_dataset"
    unprocessable_table_id = "unprocessable_claims"
    processable_table_id = "processable_claims"

    create_dataset(bigquery_client, dataset_id)

    try:
        create_claims_table_with_data(
            bigquery_client, dataset_id, unprocessable_table_id, UNPROCESSABLE_CLAIMS_ROWS
        )
        create_claims_table_with_data(
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
            total_value=PROCESSABLE_CLAIMS_TOTAL,
            relative_value=RELATIVE_VALUE,
        )

        response = dispatch_event(event, [ProcessableApprovalHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_processable_base_handler_with_wrangling_pipeline(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for ProcessableBaseHandler via wrangling pipeline.

    This test:
    1. Creates BigQuery tables with test data
    2. Triggers the handler with a pipesv2_wrangling completion event
    3. Verifies both total and relative metrics are emitted correctly
    """
    dataset_id = "test_dataset"
    unprocessable_table_id = "unprocessable_claims"
    processable_table_id = "processable_claims"

    create_dataset(bigquery_client, dataset_id)

    try:
        create_claims_table_with_data(
            bigquery_client, dataset_id, unprocessable_table_id, UNPROCESSABLE_CLAIMS_ROWS
        )
        create_claims_table_with_data(
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
            total_value=PROCESSABLE_CLAIMS_TOTAL,
            relative_value=RELATIVE_VALUE,
        )

        response = dispatch_event(event, [ProcessableWranglingHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_processable_base_handler_missing_processable_table(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for ProcessableBaseHandler when processable table doesn't exist.

    This test:
    1. Creates only the unprocessable table (processable table doesn't exist)
    2. Triggers the handler with a pipesv2_approval completion event
    3. Verifies that processable sum is assumed to be 0
    4. Verifies both total and relative metrics are emitted correctly
    """
    dataset_id = "test_dataset"
    unprocessable_table_id = "unprocessable_claims"
    processable_table_id = "processable_claims"

    create_dataset(bigquery_client, dataset_id)

    try:
        # Only create unprocessable table, not processable table
        create_claims_table_with_data(
            bigquery_client, dataset_id, unprocessable_table_id, UNPROCESSABLE_CLAIMS_ROWS
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            pipeline_uuid="pipesv2_approval",
            partner="cemig",
            unprocessable_table_id=unprocessable_table_id,
            processable_table_id=processable_table_id,  # This table doesn't exist
            unprocessable_table_var="unprocessable_claims_input_table",
            processable_table_var="processable_claims_input_table",
            include_savings_tables=True,
        )

        # Processable sum should be 0 (table doesn't exist)
        # Relative value should be 0 / (0 + 1000) = 0
        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="cemig",
            approved="true",
            total_value=0.0,  # Processable table doesn't exist, so sum is 0
            relative_value=0.0,  # 0 / (0 + 1000) = 0
        )

        response = dispatch_event(event, [ProcessableApprovalHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_processable_base_handler_missing_unprocessable_table(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for ProcessableBaseHandler when unprocessable table doesn't exist.

    This test:
    1. Creates only the processable table (unprocessable table doesn't exist)
    2. Triggers the handler with a pipesv2_wrangling completion event
    3. Verifies that unprocessable sum is assumed to be 0
    4. Verifies both total and relative metrics are emitted correctly
    """
    dataset_id = "test_dataset"
    unprocessable_table_id = "unprocessable_claims"
    processable_table_id = "processable_claims"

    create_dataset(bigquery_client, dataset_id)

    try:
        # Only create processable table, not unprocessable table
        create_claims_table_with_data(
            bigquery_client, dataset_id, processable_table_id, PROCESSABLE_CLAIMS_ROWS
        )

        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            pipeline_uuid="pipesv2_wrangling",
            partner="athena",
            unprocessable_table_id=unprocessable_table_id,  # This table doesn't exist
            processable_table_id=processable_table_id,
            unprocessable_table_var="refined_unprocessable_claims_output_table",
            processable_table_var="refined_processable_claims_output_table",
        )

        # Unprocessable sum should be 0 (table doesn't exist)
        # Relative value should be 4000 / (4000 + 0) = 1.0
        expected_calls = _create_expected_metric_calls(
            mocker,
            partner="athena",
            approved="false",
            total_value=PROCESSABLE_CLAIMS_TOTAL,  # 4000
            relative_value=1.0,  # 4000 / (4000 + 0) = 1.0
        )

        response = dispatch_event(event, [ProcessableWranglingHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


@pytest.mark.integration
def test_processable_base_handler_both_tables_missing(
    bigquery_client,
    mock_monitoring_client,
    flask_app,
    mocker: MockerFixture,
    dispatch_event,
):
    """Integration test for ProcessableBaseHandler when both tables don't exist.

    This test:
    1. Creates neither table (both tables don't exist)
    2. Triggers the handler with a pipesv2_approval completion event
    3. Verifies that only total metric is emitted (value=0.0), no relative metric
    """
    dataset_id = "test_dataset"
    unprocessable_table_id = "unprocessable_claims"
    processable_table_id = "processable_claims"

    create_dataset(bigquery_client, dataset_id)

    try:
        # Create neither table
        event = _create_cloud_event(
            bigquery_client=bigquery_client,
            dataset_id=dataset_id,
            pipeline_uuid="pipesv2_approval",
            partner="cemig",
            unprocessable_table_id=unprocessable_table_id,  # This table doesn't exist
            processable_table_id=processable_table_id,  # This table doesn't exist
            unprocessable_table_var="unprocessable_claims_input_table",
            processable_table_var="processable_claims_input_table",
            include_savings_tables=True,
        )

        # Both sums should be 0 (tables don't exist)
        # Only total metric should be emitted, not relative metric
        expected_calls = _create_expected_metric_calls_total_only(
            mocker,
            partner="cemig",
            approved="true",
            total_value=0.0,  # Both tables don't exist, so sum is 0
        )

        response = dispatch_event(event, [ProcessableApprovalHandler])

        assert_response_success(response)
        assert_metrics_emitted(mock_monitoring_client, expected_calls)

    finally:
        bigquery_client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)
