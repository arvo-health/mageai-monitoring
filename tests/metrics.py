"""Shared test utilities for metric verification."""


class MetricMatcher:
    """Custom matcher for verifying TimeSeries metric properties."""

    def __init__(self, metric_type, value, labels):
        self.metric_type = metric_type
        self.value = value
        self.labels = labels

    def __eq__(self, time_series_list):
        """Match against a list containing a single TimeSeries."""
        if len(time_series_list) != 1:
            return False
        ts = time_series_list[0]
        return (
            ts.metric.type == f"custom.googleapis.com/{self.metric_type}"
            and dict(ts.metric.labels) == self.labels
            and len(ts.points) == 1
            and ts.points[0].value.double_value == self.value
        )

    def __repr__(self):
        return f"MetricMatcher(type={self.metric_type}, value={self.value}, labels={self.labels})"


def assert_metrics_emitted(mock_monitoring_client, expected_calls: list) -> None:
    """Assert that expected metric calls were made."""
    actual_calls = mock_monitoring_client.create_time_series.call_args_list

    for expected_call in expected_calls:
        expected_name = expected_call.kwargs["name"]
        expected_time_series_matcher = expected_call.kwargs["time_series"]

        found = False
        for actual_call in actual_calls:
            if actual_call.kwargs.get("name") == expected_name:
                actual_time_series = actual_call.kwargs.get("time_series", [])
                if expected_time_series_matcher == actual_time_series:
                    found = True
                    break

        assert found, (
            f"Expected call not found: name={expected_name}, "
            f"time_series={expected_time_series_matcher}"
        )
