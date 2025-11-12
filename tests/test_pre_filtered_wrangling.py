"""Unit tests for PreFilteredWranglingHandler."""

from pytest_mock import MockerFixture

from handlers.pre_filtered_wrangling import PreFilteredWranglingHandler


def test_match_with_wrangling_completed(mocker: MockerFixture):
    """Test that match returns True for pipesv2_wrangling completion events."""
    handler = PreFilteredWranglingHandler(
        monitoring_client=mocker.MagicMock(),
        bq_client=mocker.MagicMock(),
        run_project_id="test-project",
        data_project_id="test-data-project",
    )

    decoded_message = {
        "payload": {
            "pipeline_uuid": "pipesv2_wrangling",
            "status": "COMPLETED",
        }
    }

    assert handler.match(decoded_message) is True


def test_match_with_wrong_pipeline(mocker: MockerFixture):
    """Test that match returns False for non-wrangling pipelines."""
    handler = PreFilteredWranglingHandler(
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

    assert handler.match(decoded_message) is False


def test_match_with_wrong_status(mocker: MockerFixture):
    """Test that match returns False for non-completed statuses."""
    handler = PreFilteredWranglingHandler(
        monitoring_client=mocker.MagicMock(),
        bq_client=mocker.MagicMock(),
        run_project_id="test-project",
        data_project_id="test-data-project",
    )

    decoded_message = {
        "payload": {
            "pipeline_uuid": "pipesv2_wrangling",
            "status": "RUNNING",
        }
    }

    assert handler.match(decoded_message) is False


def test_match_with_missing_payload(mocker: MockerFixture):
    """Test that match returns False when payload is missing."""
    handler = PreFilteredWranglingHandler(
        monitoring_client=mocker.MagicMock(),
        bq_client=mocker.MagicMock(),
        run_project_id="test-project",
        data_project_id="test-data-project",
    )

    decoded_message = {}

    assert handler.match(decoded_message) is False


def test_handle_delegates_to_base(mocker: MockerFixture):
    """Test that handle delegates to _handle_pre_filtered_metrics with correct parameters."""
    handler = PreFilteredWranglingHandler(
        monitoring_client=mocker.MagicMock(),
        bq_client=mocker.MagicMock(),
        run_project_id="test-project",
        data_project_id="test-data-project",
    )

    decoded_message = {
        "source_timestamp": "2024-01-15T10:30:00Z",
        "payload": {
            "pipeline_uuid": "pipesv2_wrangling",
            "status": "COMPLETED",
            "variables": {
                "partner": "abertta",
                "refined_unprocessable_claims_output_table": "dataset.unprocessable",
                "refined_processable_claims_output_table": "dataset.processable",
            },
        },
    }

    # Mock the base handler method
    mock_handle_pre_filtered_metrics = mocker.patch.object(handler, "_handle_pre_filtered_metrics")
    handler.handle(decoded_message)

    # Verify it was called with the correct parameters
    mock_handle_pre_filtered_metrics.assert_called_once_with(
        decoded_message=decoded_message,
        pipeline_uuid="pipesv2_wrangling",
        unprocessable_table_var="refined_unprocessable_claims_output_table",
        processable_table_var="refined_processable_claims_output_table",
        approved_value="false",
    )
