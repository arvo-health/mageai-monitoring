"""Utility functions for emitting GCP Cloud Monitoring metrics."""

from datetime import datetime
from google.cloud import monitoring_v3
from google.cloud.monitoring_v3 import TimeSeries, Point


def emit_gauge_metric(
    *,
    monitoring_client: monitoring_v3.MetricServiceClient,
    project_id: str,
    name: str,
    value: float,
    labels: dict,
    timestamp: datetime,
):
    """
    Helper to emit a GAUGE metric safely and idempotently.
    
    Args:
        monitoring_client: GCP Monitoring client
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

    monitoring_client.create_time_series(name=project_name, time_series=[series])

