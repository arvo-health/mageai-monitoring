"""Unit tests for PostFilteredApprovalHandler."""

from pytest_mock import MockerFixture

from handlers.post_filtered_approval import PostFilteredApprovalHandler


def test_match_with_approval_completed(mocker: MockerFixture):
    """Test that match returns True for pipesv2_approval completion events."""
    handler = PostFilteredApprovalHandler(
        monitoring_client=mocker.MagicMock(),
        bq_client=mocker.MagicMock(),
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


def test_match_with_wrong_pipeline(mocker: MockerFixture):
    """Test that match returns False for non-approval pipelines."""
    handler = PostFilteredApprovalHandler(
        monitoring_client=mocker.MagicMock(),
        bq_client=mocker.MagicMock(),
        run_project_id="test-project",
        data_project_id="test-data-project",
    )

    decoded_message = {
        "payload": {
            "pipeline_uuid": "pipesv2_selection",
            "status": "COMPLETED",
        }
    }

    assert handler.match(decoded_message) is False


def test_match_with_wrong_status(mocker: MockerFixture):
    """Test that match returns False for non-completed statuses."""
    handler = PostFilteredApprovalHandler(
        monitoring_client=mocker.MagicMock(),
        bq_client=mocker.MagicMock(),
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


def test_match_with_missing_payload(mocker: MockerFixture):
    """Test that match returns False when payload is missing."""
    handler = PostFilteredApprovalHandler(
        monitoring_client=mocker.MagicMock(),
        bq_client=mocker.MagicMock(),
        run_project_id="test-project",
        data_project_id="test-data-project",
    )

    decoded_message = {}

    assert handler.match(decoded_message) is False


def test_handle_delegates_to_base(mocker: MockerFixture):
    """Test that handle delegates to _handle_post_filtered_metrics with correct parameters."""
    handler = PostFilteredApprovalHandler(
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
                "partner": "cemig",
                "unprocessable_claims_input_table": "dataset.unprocessable",
                "processable_claims_input_table": "dataset.processable",
                "excluded_savings_input_table": "dataset.excluded",
                "savings_input_table": "dataset.savings",
            },
        },
    }

    # Mock the base handler method
    mock_handle_post_filtered_metrics = mocker.patch.object(
        handler, "_handle_post_filtered_metrics"
    )
    handler.handle(decoded_message)

    # Verify it was called with the correct parameters
    mock_handle_post_filtered_metrics.assert_called_once_with(
        decoded_message=decoded_message,
        pipeline_uuid="pipesv2_approval",
        excluded_table_var="excluded_savings_input_table",
        savings_table_var="savings_input_table",
        approved_value="true",
    )
