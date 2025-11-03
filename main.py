import base64
import json
import logging
from datetime import datetime

import functions_framework
from flask import make_response
from google.cloud import monitoring_v3
from google.cloud import bigquery

# --- Configuration ---
RUN_PROJECT_ID = "arvo-eng-prd"  # Cloud Run project (for metrics)
DATA_PROJECT_ID = "arvo-data-platform-prd"  # BigQuery data project

monitoring_client = monitoring_v3.MetricServiceClient()
bq_client = bigquery.Client(project=DATA_PROJECT_ID)

logging.basicConfig(level=logging.INFO)


@functions_framework.cloud_event
def log_and_metric_pubsub(cloud_event):
    """Triggered by Pub/Sub message. Logs and emits metrics."""
    try:
        message = cloud_event.data.get("message", {})
        data = message.get("data")

        if not data:
            logging.warning("No data in Pub/Sub message.")
            return make_response(("No data in message", 400))

        # Decode and log
        decoded = base64.b64decode(data).decode("utf-8")
        logging.info(f"Full message: {decoded}")

        try:
            payload = json.loads(decoded)
        except json.JSONDecodeError:
            logging.error("Invalid JSON in Pub/Sub message.")
            return make_response(("Invalid JSON", 400))

        # Extract base metric info
        try:
            pl = payload["payload"]
            variables = pl.get("variables", {})

            labels = {
                "pipeline_uuid": pl["pipeline_uuid"],
                "pipeline_status": pl["status"],
            }

            # Partner label (always fetched from payload.payload.variables.partner)
            partner_value = variables.get("partner")
            if partner_value:
                labels["partner"] = str(partner_value)

            source_timestamp = datetime.fromisoformat(
                payload["source_timestamp"].replace("Z", "+00:00")
            )

        except KeyError as e:
            logging.error(f"Missing key in payload: {e}")
            return make_response(("Invalid payload structure", 400))

        # --- Emit primary metric ---
        emit_gauge_metric(
            project_id=RUN_PROJECT_ID,
            name="mageai_pipeline_run",
            value=1.0,
            labels=labels,
            timestamp=source_timestamp,
        )

        # --- Extra behavior for specific pipeline ---
        if (
            pl["pipeline_uuid"] == "pipesv2_approval"
            and pl["status"] == "COMPLETED"
        ):
            logging.info(
                "Pipeline pipesv2_approval COMPLETED — triggering BigQuery aggregation..."
            )

            input_table = variables.get("unprocessable_claims_input_table")

            if not input_table:
                logging.warning(
                    "No variable 'unprocessable_claims_input_table' found in payload."
                )
                return make_response(("Missing input table variable", 400))

            # Ensure fully-qualified table path
            full_table_ref = (
                input_table
                if "." in input_table
                else f"{DATA_PROJECT_ID}.{input_table}"
            )

            # Query BigQuery
            query = f"SELECT SUM(vl_pago) AS total_vl_pago FROM `{full_table_ref}`"
            query_job = bq_client.query(query)
            result = query_job.result()
            row = next(result, None)
            total_vl_pago = float(row.total_vl_pago or 0.0)

            logging.info(
                f"Computed total_vl_pago={total_vl_pago} from {full_table_ref}"
            )

            # Emit BigQuery-derived metric with same labels (including partner)
            emit_gauge_metric(
                project_id=RUN_PROJECT_ID,
                name="claims_unprocessable_claims_total_vl_pago",
                value=total_vl_pago,
                labels=labels,
                timestamp=source_timestamp,
            )

        # If everything succeeded, return 200
        return make_response(("OK", 200))

    except Exception as e:
        logging.error(f"Unhandled error: {e}", exc_info=True)
        # Returning 500 signals Pub/Sub to retry delivery
        return make_response(("Internal Server Error", 500))


def emit_gauge_metric(*, project_id: str, name: str, value: float, labels: dict, timestamp: datetime):
    """Helper to emit a GAUGE metric safely and idempotently."""
    metric_type = f"custom.googleapis.com/{name}"
    project_name = f"projects/{project_id}"

    series = monitoring_v3.TimeSeries()
    series.metric.type = metric_type
    series.metric.labels.update(labels)
    series.resource.type = "global"
    series.resource.labels["project_id"] = project_id

    point = monitoring_v3.Point()
    point.value.double_value = float(value)
    point.interval.end_time = timestamp.isoformat()
    series.points = [point]

    try:
        monitoring_client.create_time_series(name=project_name, time_series=[series])
        logging.info(f"Metric {name} emitted (value={value}) with labels={labels}.")
    except Exception as e:
        # Propagate exceptions so that handler can return 500 and trigger retry
        logging.error(f"Failed to write metric {name}: {e}")
        raise
