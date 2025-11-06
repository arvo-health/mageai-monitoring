"""Metrics emission utilities."""

import json
import logging
from datetime import datetime

from google.cloud import monitoring_v3
from google.cloud.monitoring_v3 import Point, TimeSeries

from config import Config

from config import Config


def emit_gauge_metric(
    *,
    monitoring_client: monitoring_v3.MetricServiceClient,
    project_id: str,
    name: str,
    value: float,
    labels: dict,
    timestamp: datetime,
) -> None:
    """
    Emit a GAUGE metric to GCP Cloud Monitoring or log it (local mode).

    Args:
        monitoring_client: GCP Monitoring client (may be monkey-patched for local mode)
        project_id: Project ID for metric emission
        name: Metric name
        value: Metric value
        labels: Metric labels
        timestamp: Metric timestamp
    """
    metric_type = f"custom.googleapis.com/{name}"
    project_name = f"projects/{project_id}"

    series = TimeSeries()
    series.metric.type = metric_type
    series.metric.labels.update(labels)
    series.resource.type = "global"
    series.resource.labels["project_id"] = project_id

    point = Point()
    point.value.double_value = float(value)
    point.interval.end_time = timestamp.isoformat()
    series.points = [point]

    # In local mode, monitoring_client.create_time_series is monkey-patched to log
    # In production mode, it's the real method
    monitoring_client.create_time_series(name=project_name, time_series=[series])


def _create_logging_monitoring_client() -> monitoring_v3.MetricServiceClient:
    """
    Create a monitoring client with monkey-patched methods that log instead of calling GCP.

    Returns:
        Monitoring client with logging methods
    """
    client = monitoring_v3.MetricServiceClient()

    # Store original method

    def logged_create_time_series(name: str, time_series: list) -> None:
        """Log metric data instead of sending to GCP."""
        for ts in time_series:
            # Convert timestamp to ISO format string for JSON serialization
            timestamp_str = None
            if ts.points and ts.points[0].interval.end_time:
                end_time = ts.points[0].interval.end_time
                # Handle both string and DatetimeWithNanoseconds objects
                if isinstance(end_time, str):
                    timestamp_str = end_time
                elif hasattr(end_time, "isoformat"):
                    # Use isoformat() if available (for datetime objects)
                    timestamp_str = end_time.isoformat()
                elif hasattr(end_time, "ToDatetime"):
                    # Convert DatetimeWithNanoseconds to datetime then ISO format
                    timestamp_str = end_time.ToDatetime().isoformat()
                else:
                    # Fallback to string conversion
                    timestamp_str = str(end_time)

            metric_data = {
                "metric_type": "gauge",
                "project_id": name.replace("projects/", ""),
                "name": ts.metric.type.replace("custom.googleapis.com/", ""),
                "value": ts.points[0].value.double_value if ts.points else 0.0,
                "labels": dict(ts.metric.labels),
                "timestamp": timestamp_str,
            }
            logging.info(f"METRIC: {json.dumps(metric_data)}")
        # Don't call the original method

    # Monkey-patch the method
    client.create_time_series = logged_create_time_series

    return client


def create_monitoring_client(config: Config) -> monitoring_v3.MetricServiceClient:
    """
    Create a monitoring client appropriate for the current mode.

    In local mode, returns a client with monkey-patched methods that log.
    In production mode, returns a standard GCP Monitoring client.

    Args:
        config: Application configuration

    Returns:
        Monitoring client instance
    """
    if config.local_mode:
        return _create_logging_monitoring_client()
    return monitoring_v3.MetricServiceClient()


__all__ = ["emit_gauge_metric", "create_monitoring_client"]
