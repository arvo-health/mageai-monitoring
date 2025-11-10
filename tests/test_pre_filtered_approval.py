"""Unit tests for PreFilteredApprovalHandler."""

from unittest.mock import MagicMock, patch

from handlers.pre_filtered_approval import PreFilteredApprovalHandler


def test_match_with_approval_completed():
    """Test that match returns True for pipesv2_approval completion events."""
    handler = PreFilteredApprovalHandler(
        monitoring_client=MagicMock(),
        bq_client=MagicMock(),
        run_project_id="test-project",
        data_project_id="test-data-project",
    )

    decoded_message = {
        "payload": {
            "pipeline_uuid": "pipesv2_approval",
            "status": "COMPLETED",
        }
    }

    assert handler.match(decoded_message) is True


def test_match_with_wrong_pipeline():
    """Test that match returns False for non-approval pipelines."""
    handler = PreFilteredApprovalHandler(
        monitoring_client=MagicMock(),
        bq_client=MagicMock(),
        run_project_id="test-project",
        data_project_id="test-data-project",
    )

    decoded_message = {
        "payload": {
            "pipeline_uuid": "pipesv2_wrangling",
            "status": "COMPLETED",
        }
    }

    assert handler.match(decoded_message) is False


def test_match_with_wrong_status():
    """Test that match returns False for non-completed statuses."""
    handler = PreFilteredApprovalHandler(
        monitoring_client=MagicMock(),
        bq_client=MagicMock(),
        run_project_id="test-project",
        data_project_id="test-data-project",
    )

    decoded_message = {
        "payload": {
            "pipeline_uuid": "pipesv2_approval",
            "status": "RUNNING",
        }
    }

    assert handler.match(decoded_message) is False


def test_match_with_missing_payload():
    """Test that match returns False when payload is missing."""
    handler = PreFilteredApprovalHandler(
        monitoring_client=MagicMock(),
        bq_client=MagicMock(),
        run_project_id="test-project",
        data_project_id="test-data-project",
    )

    decoded_message = {}

    assert handler.match(decoded_message) is False


def test_handle_delegates_to_base():
    """Test that handle delegates to _handle_pre_filtered_metrics with correct parameters."""
    handler = PreFilteredApprovalHandler(
        monitoring_client=MagicMock(),
        bq_client=MagicMock(),
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
                "unprocessable_claims_input_table": "dataset.unprocessable",
                "processable_claims_input_table": "dataset.processable",
            },
        },
    }

    # Mock the base handler method
    with patch.object(
        handler, "_handle_pre_filtered_metrics"
    ) as mock_handle_pre_filtered_metrics:
        handler.handle(decoded_message)

        # Verify it was called with the correct parameters
        mock_handle_pre_filtered_metrics.assert_called_once_with(
            decoded_message=decoded_message,
            pipeline_uuid="pipesv2_approval",
            unprocessable_table_var="unprocessable_claims_input_table",
            processable_table_var="processable_claims_input_table",
            approved_value="true",
        )
