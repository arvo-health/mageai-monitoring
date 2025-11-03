"""Unit tests for monitoring metric emission."""

import base64
import json
from datetime import datetime
import pytest
from cloudevents.http import CloudEvent
# Import the function to test
import main


@pytest.mark.parametrize(
    "payload,expected_labels,expected_status",
    [
        pytest.param(
            {
                "source_timestamp": "2024-01-15T10:30:00Z",
                "payload": {
                    "pipeline_uuid": "test_pipeline_123",
                    "status": "COMPLETED",
                    "variables": {
                        "partner": "test_partner",
                        "unprocessable_claims_input_table": "test_dataset.test_table",
                    },
                },
            },
            {
                "pipeline_uuid": "test_pipeline_123",
                "pipeline_status": "COMPLETED",
                "partner": "test_partner",
            },
            200,
            id="with_partner",
        ),
        pytest.param(
            {
                "source_timestamp": "2024-01-15T10:30:00Z",
                "payload": {
                    "pipeline_uuid": "test_pipeline_456",
                    "status": "FAILED",
                    "variables": {},
                },
            },
            {
                "pipeline_uuid": "test_pipeline_456",
                "pipeline_status": "FAILED",
            },
            200,
            id="without_partner",
        ),
    ],
)
def test_mageai_pipeline_run_metric_emission(
    mock_monitoring_client,
    flask_app,
    payload,
    expected_labels,
    expected_status,
):
    """
    Test metric emission for mageai_pipeline_run scenarios.
    
    This test verifies:
    1. The Cloud Run function correctly parses Pub/Sub messages
    2. The metric is emitted with correct name, value, labels, and timestamp
    3. The function handles partner labels correctly (present or absent)
    4. The function returns appropriate HTTP responses
    """
    # Create CloudEvent
    encoded_data = base64.b64encode(
        json.dumps(payload).encode("utf-8")
    ).decode("utf-8")
    
    event = CloudEvent(
        {
            "type": "google.cloud.pubsub.topic.v1.messagePublished",
            "source": "//pubsub.googleapis.com/projects/test-project/topics/test-topic",
            "specversion": "1.0",
            "id": "test-event-id",
        },
        {
            "message": {
                "data": encoded_data,
                "messageId": "test-message-id",
                "publishTime": "2024-01-15T10:30:00Z",
            }
        },
    )
    
    # Call the function
    response = main.log_and_metric_pubsub(event)
    
    # Verify HTTP response
    assert response is not None
    status_code = response[1] if isinstance(response, tuple) else response.status_code
    assert status_code == expected_status
    
    # Verify monitoring client was called exactly once
    assert mock_monitoring_client.create_time_series.call_count == 1
    assert len(mock_monitoring_client.captured_calls) == 1
    
    # Extract the captured call
    call = mock_monitoring_client.captured_calls[0]
    
    # Verify project name format
    assert call["project_name"] == "projects/arvo-eng-prd"
    
    # Verify exactly one time series was sent
    assert len(call["time_series"]) == 1
    
    time_series = call["time_series"][0]
    
    # Verify metric type
    assert time_series.metric.type == "custom.googleapis.com/mageai_pipeline_run"
    
    # Verify metric labels
    labels = dict(time_series.metric.labels)
    for key, value in expected_labels.items():
        assert labels[key] == value
    
    # Verify partner label handling
    if "partner" in expected_labels:
        assert "partner" in labels
        assert labels["partner"] == expected_labels["partner"]
    else:
        assert "partner" not in labels
    
    # Verify resource labels
    resource_labels = dict(time_series.resource.labels)
    assert resource_labels["project_id"] == "arvo-eng-prd"
    assert time_series.resource.type == "global"
    
    # Verify metric value
    assert len(time_series.points) == 1
    point = time_series.points[0]
    assert point.value.double_value == 1.0
    
    # Verify timestamp (should match source_timestamp from payload)
    assert point.interval.end_time is not None


@pytest.mark.parametrize(
    "payload_data,expected_status",
    [
        pytest.param(
            "not-valid-base64!",
            500,
            id="invalid_base64",
        ),
        pytest.param(
            base64.b64encode(
                json.dumps({
                    "source_timestamp": "2024-01-15T10:30:00Z",
                    # Missing "payload" field
                }).encode("utf-8")
            ).decode("utf-8"),
            400,
            id="missing_required_fields",
        ),
    ],
)
def test_mageai_pipeline_run_error_handling(
    mock_monitoring_client,
    flask_app,
    payload_data,
    expected_status,
):
    """
    Test that the function handles various error scenarios gracefully.
    """
    # Create CloudEvent with problematic data
    event = CloudEvent(
        {
            "type": "google.cloud.pubsub.topic.v1.messagePublished",
            "source": "//pubsub.googleapis.com/projects/test-project/topics/test-topic",
            "specversion": "1.0",
            "id": "test-event-id",
        },
        {
            "message": {
                "data": payload_data,
                "messageId": "test-message-id",
            }
        },
    )
    
    # Call the function
    response = main.log_and_metric_pubsub(event)
    
    # Verify error response
    assert response is not None
    status_code = response[1] if isinstance(response, tuple) else response.status_code
    assert status_code == expected_status
    
    # Verify no metrics were emitted
    assert len(mock_monitoring_client.captured_calls) == 0

