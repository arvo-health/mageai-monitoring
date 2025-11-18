"""Base handler for processable handlers.

This module contains the base class for handlers that calculate and emit
processable claims metrics from pipeline completion events.
"""

from datetime import datetime

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, monitoring_v3

from handlers.base import Handler, HandlerBadRequestError
from metrics import emit_gauge_metric


class ProcessableBaseHandler(Handler):
    """
    Base handler for calculating and emitting processable claims metrics.

    This base class provides common functionality for handlers that process
    pipeline completion events to compute aggregate metrics from processable
    claims results. Subclasses should implement the `match` method and call
    `_handle_processable_metrics` with pipeline-specific configuration.
    """

    def __init__(
        self,
        monitoring_client: monitoring_v3.MetricServiceClient,
        bq_client: bigquery.Client,
        run_project_id: str,
        data_project_id: str,
    ):
        """
        Initialize the handler.

        Args:
            monitoring_client: Monitoring client (GCP or logged)
            bq_client: BigQuery client
            run_project_id: Project ID for metric emission
            data_project_id: Project ID for BigQuery data
        """
        self.monitoring_client = monitoring_client
        self.bq_client = bq_client
        self.run_project_id = run_project_id
        self.data_project_id = data_project_id

    def _handle_processable_metrics(
        self,
        decoded_message: dict,
        pipeline_uuid: str,
        processable_table_var: str,
        unprocessable_table_var: str,
        approved_value: str,
    ) -> None:
        """
        Calculate processable claims metrics and emit to Cloud Monitoring.

        Queries BigQuery to aggregate the total value of processable claims,
        then emits metrics representing both the total and relative values.
        The relative metric is calculated as the ratio of processable claims
        value to the total value of all claims (processable + unprocessable).

        If the processable table doesn't exist, assumes its sum is 0.
        If the unprocessable table doesn't exist, assumes its sum is 0.
        If both tables don't exist (both sums are 0), emits only the total metric,
        not the relative metric.

        Args:
            decoded_message: The decoded message dictionary containing pipeline completion data
            pipeline_uuid: The pipeline UUID to verify (for defensive check)
            processable_table_var: Variable name for the processable claims table
            unprocessable_table_var: Variable name for the unprocessable claims table
            approved_value: Value for the "approved" label ("true" or "false")

        Raises:
            HandlerBadRequestError: If the required input table variables are not present
                in the event, or if the payload is missing
        """
        payload = decoded_message.get("payload")

        if not payload:
            raise HandlerBadRequestError("No 'payload' found in event data.")

        variables = payload.get("variables", {})

        # Verify we're still handling the right event (defensive check)
        if payload.get("pipeline_uuid") != pipeline_uuid or payload.get("status") != "COMPLETED":
            return

        processable_table = variables.get(processable_table_var)
        unprocessable_table = variables.get(unprocessable_table_var)

        if not processable_table:
            raise HandlerBadRequestError(f"No variable '{processable_table_var}' found in payload.")

        if not unprocessable_table:
            raise HandlerBadRequestError(
                f"No variable '{unprocessable_table_var}' found in payload."
            )

        # Ensure fully-qualified table paths
        def ensure_full_table_ref(table: str) -> str:
            return table if "." in table else f"{self.data_project_id}.{table}"

        full_processable_table = ensure_full_table_ref(processable_table)
        full_unprocessable_table = ensure_full_table_ref(unprocessable_table)

        # Check if processable table exists and query it separately
        # If processable table doesn't exist, assume sum is 0
        total_vl_pago = 0.0
        try:
            # Check if table exists first to avoid hanging on query
            self.bq_client.get_table(full_processable_table)
            # Table exists, so query it
            processable_query = f"""
            SELECT COALESCE(SUM(vl_pago), 0) AS total_vl_pago
            FROM `{full_processable_table}`
            """
            query_job = self.bq_client.query(processable_query)
            result = query_job.result()
            row = next(result, None)
            if row:
                total_vl_pago = float(row.total_vl_pago or 0.0)
        except NotFound:
            # If table doesn't exist, assume sum is 0
            total_vl_pago = 0.0

        # Check if unprocessable table exists and query it separately
        # If unprocessable table doesn't exist, assume sum is 0
        sum_unprocessable_vl_pago = 0.0
        try:
            # Check if table exists first to avoid hanging on query
            self.bq_client.get_table(full_unprocessable_table)
            # Table exists, so query it
            unprocessable_query = f"""
            SELECT COALESCE(SUM(vl_pago), 0) AS sum_unprocessable_vl_pago
            FROM `{full_unprocessable_table}`
            """
            query_job = self.bq_client.query(unprocessable_query)
            result = query_job.result()
            row = next(result, None)
            if row:
                sum_unprocessable_vl_pago = float(row.sum_unprocessable_vl_pago or 0.0)
        except NotFound:
            # If table doesn't exist, assume sum is 0
            sum_unprocessable_vl_pago = 0.0

        # Calculate relative value
        total_sum = total_vl_pago + sum_unprocessable_vl_pago
        if total_sum > 0:
            relative_vl_pago = total_vl_pago / total_sum
        else:
            relative_vl_pago = 0.0

        # Build labels: partner and approved
        labels = {
            "approved": approved_value,
        }

        # Partner label is required
        partner_value = variables.get("partner")
        if not partner_value:
            raise HandlerBadRequestError("Missing required 'partner' variable in payload.")
        labels["partner"] = str(partner_value)

        # Parse timestamp
        source_timestamp = datetime.fromisoformat(
            decoded_message["source_timestamp"].replace("Z", "+00:00")
        )

        # Emit metric representing total value of processable claims
        emit_gauge_metric(
            monitoring_client=self.monitoring_client,
            project_id=self.run_project_id,
            name="claims/pipeline/processable/vl_pago/total",
            value=total_vl_pago,
            labels=labels,
            timestamp=source_timestamp,
        )

        # Emit metric representing relative value of processable claims
        # Only emit relative metric if at least one table exists (total_sum > 0)
        # If both tables don't exist (both sums are 0), skip relative metric
        if total_sum > 0:
            emit_gauge_metric(
                monitoring_client=self.monitoring_client,
                project_id=self.run_project_id,
                name="claims/pipeline/processable/vl_pago/relative",
                value=relative_vl_pago,
                labels=labels,
                timestamp=source_timestamp,
            )
