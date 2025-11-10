"""Handler for calculating and emitting pre-processing filter metrics.

This handler processes completion events from the pipesv2_approval pipeline
to compute aggregate metrics from pre-processing filter results. It queries
BigQuery to aggregate claim values and emits metrics that track the total
value of claims that were filtered during the pre-processing stage.
"""

from datetime import datetime

from google.cloud import bigquery, monitoring_v3

from handlers.base import Handler, HandlerBadRequestError
from metrics import emit_gauge_metric


class PreFilteredApprovalHandler(Handler):
    """
    Calculates and emits metrics for pre-processing filter results.

    When the pipesv2_approval pipeline completes, this handler queries BigQuery
    to aggregate claim values from the pre-processing filter stage and emits
    two metrics: one representing the total value of filtered claims, and another
    representing the relative value (ratio of filtered claims to processable claims).
    This enables monitoring of the financial impact of pre-processing filters applied
    during the approval pipeline execution.
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

    def match(self, decoded_message: dict) -> bool:
        """
        Determine if this handler should process the event.

        Matches events representing successful completion of the pipesv2_approval
        pipeline, which triggers the calculation of pre-processing filter metrics.

        Args:
            decoded_message: The decoded message dictionary to check

        Returns:
            True if this is a pipesv2_approval pipeline completion event,
            False otherwise
        """
        pl = decoded_message.get("payload", {})
        pipeline_uuid = pl.get("pipeline_uuid")
        pipeline_status = pl.get("status")

        return pipeline_uuid == "pipesv2_approval" and pipeline_status == "COMPLETED"

    def handle(self, decoded_message: dict) -> None:
        """
        Calculate pre-processing filter metrics and emit to Cloud Monitoring.

        Queries BigQuery to aggregate the total value of claims filtered during
        the pre-processing stage, then emits metrics representing both the total
        and relative values. The relative metric is calculated as the ratio of
        filtered claims value to the total value of processable claims.

        Args:
            decoded_message: The decoded message dictionary containing pipeline completion data

        Raises:
            HandlerBadRequestError: If the required input table variables
                (unprocessable_claims_input_table or processable_claims_input_table)
                are not present in the event
        """
        pl = decoded_message["payload"]
        variables = pl.get("variables", {})

        # Verify we're still handling the right event (defensive check)
        if pl.get("pipeline_uuid") != "pipesv2_approval" or pl.get("status") != "COMPLETED":
            return

        unprocessable_table = variables.get("unprocessable_claims_input_table")
        processable_table = variables.get("processable_claims_input_table")

        if not unprocessable_table:
            raise HandlerBadRequestError(
                "No variable 'unprocessable_claims_input_table' found in payload."
            )

        if not processable_table:
            raise HandlerBadRequestError(
                "No variable 'processable_claims_input_table' found in payload."
            )

        # Ensure fully-qualified table paths
        def ensure_full_table_ref(table: str) -> str:
            return table if "." in table else f"{self.data_project_id}.{table}"

        full_unprocessable_table = ensure_full_table_ref(unprocessable_table)
        full_processable_table = ensure_full_table_ref(processable_table)

        # Single BigQuery query to get both values
        query = f"""
        WITH unprocessable_sum AS (
            SELECT COALESCE(SUM(vl_pago), 0) AS total_vl_pago
            FROM `{full_unprocessable_table}`
        ),
        processable_sum AS (
            SELECT COALESCE(SUM(vl_pago), 0) AS sum_processable_vl_pago
            FROM `{full_processable_table}`
        )
        SELECT
            u.total_vl_pago,
            p.sum_processable_vl_pago,
            CASE
                WHEN p.sum_processable_vl_pago > 0
                THEN u.total_vl_pago / p.sum_processable_vl_pago
                ELSE 0
            END AS relative_vl_pago
        FROM unprocessable_sum u, processable_sum p
        """

        query_job = self.bq_client.query(query)
        result = query_job.result()
        row = next(result, None)

        total_vl_pago = float(row.total_vl_pago or 0.0)
        relative_vl_pago = float(row.relative_vl_pago or 0.0)

        # Build labels: only partner and approved=true
        labels = {
            "approved": "true",
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

        # Emit metric representing total value of claims filtered in pre-processing
        emit_gauge_metric(
            monitoring_client=self.monitoring_client,
            project_id=self.run_project_id,
            name="claims/pipeline/filtered_pre/vl_pago/total",
            value=total_vl_pago,
            labels=labels,
            timestamp=source_timestamp,
        )

        # Emit metric representing relative value of claims filtered in pre-processing
        emit_gauge_metric(
            monitoring_client=self.monitoring_client,
            project_id=self.run_project_id,
            name="claims/pipeline/filtered_pre/vl_pago/relative",
            value=relative_vl_pago,
            labels=labels,
            timestamp=source_timestamp,
        )
