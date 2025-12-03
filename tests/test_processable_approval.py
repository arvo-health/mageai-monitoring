"""Unit tests for ProcessableApprovalHandler."""

import pytest
from pytest_mock import MockerFixture

from handlers.processable_approval import ProcessableApprovalHandler


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
    handler = ProcessableApprovalHandler(
        monitoring_client=mocker.MagicMock(),
        bq_client=mocker.MagicMock(),
        run_project_id="test-project",
        data_project_id="test-data-project",
    )

    assert handler.match(decoded_message) is expected


def test_handle_delegates_to_base(mocker: MockerFixture):
    """Test that handle delegates to _handle_processable_metrics with correct parameters."""
    handler = ProcessableApprovalHandler(
        monitoring_client=mocker.MagicMock(),
        bq_client=mocker.MagicMock(),
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
                "processable_claims_input_table": "dataset.processable",
                "unprocessable_claims_input_table": "dataset.unprocessable",
                "excluded_savings_input_table": "dataset.excluded",
                "savings_input_table": "dataset.savings",
            },
        },
    }

    # Mock the base handler method
    mock_handle_processable_metrics = mocker.patch.object(handler, "_handle_processable_metrics")
    handler.handle(decoded_message)

    # Verify it was called with the correct parameters
    mock_handle_processable_metrics.assert_called_once_with(
        decoded_message=decoded_message,
        pipeline_uuid="pipesv2_approval",
        processable_table_var="processable_claims_input_table",
        unprocessable_table_var="unprocessable_claims_input_table",
        approved_value="true",
    )
