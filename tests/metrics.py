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
