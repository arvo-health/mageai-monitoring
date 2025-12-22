"""Base handler for savings handlers.

This module contains the base class for handlers that calculate and emit
savings metrics grouped by agent_id from pipeline completion events.
"""

from datetime import datetime

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, monitoring_v3

from handlers.base import Handler, HandlerBadRequestError
from metrics import emit_gauge_metric


class SavingsBaseHandler(Handler):
    """
    Base handler for calculating and emitting savings metrics grouped by agent_id.

    This base class provides common functionality for handlers that process
    pipeline completion events to compute aggregate metrics from savings results
    grouped by agent_id. Subclasses should implement the `match` method and call
    `_handle_savings_metrics` with pipeline-specific configuration.
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

    def _handle_savings_metrics(
        self,
        decoded_message: dict,
        pipeline_uuid: str,
        savings_table_var: str,
        approved_value: str,
    ) -> None:
        """
        Calculate savings metrics per agent_id and emit to Cloud Monitoring.

        Queries BigQuery to aggregate the total value and count of savings
        grouped by agent_id, then emits two metrics per agent_id: amount and count.
        If the table doesn't exist, no metrics are emitted.

        Args:
            decoded_message: The decoded message dictionary containing pipeline completion data
            pipeline_uuid: The pipeline UUID to verify (for defensive check)
            savings_table_var: Variable name for the savings table
            approved_value: Value for the "approved" label ("true" or "false")

        Raises:
            HandlerBadRequestError: If the required input table variable or partner
                variable are not present in the event, or if the payload is missing
        """
        payload = decoded_message.get("payload")

        if not payload:
            raise HandlerBadRequestError("No 'payload' found in event data.")

        variables = payload.get("variables", {})

        # Verify we're still handling the right event (defensive check)
        if payload.get("pipeline_uuid") != pipeline_uuid or payload.get("status") != "COMPLETED":
            return

        savings_table = variables.get(savings_table_var)

        if not savings_table:
            raise HandlerBadRequestError(f"No variable '{savings_table_var}' found in payload.")

        # Partner label is required
        partner_value = variables.get("partner")
        if not partner_value:
            raise HandlerBadRequestError("Missing required 'partner' variable in payload.")

        # Ensure fully-qualified table path
        def ensure_full_table_ref(table: str) -> str:
            return table if "." in table else f"{self.data_project_id}.{table}"

        full_savings_table = ensure_full_table_ref(savings_table)

        # Parse timestamp
        source_timestamp = datetime.fromisoformat(
            decoded_message["source_timestamp"].replace("Z", "+00:00")
        )

        # Query BigQuery to get sum and count of vl_glosa_arvo grouped by agent_id
        # If table doesn't exist, don't emit any metrics
        try:
            # Check if table exists first to avoid hanging on query
            self.bq_client.get_table(full_savings_table)
            # Table exists, so query it
            savings_query = f"""
            SELECT
                agent_id,
                COALESCE(SUM(vl_glosa_arvo), 0) AS total_amount,
                COUNT(*) AS row_count
            FROM `{full_savings_table}`
            GROUP BY agent_id
            """
            query_job = self.bq_client.query(savings_query)
            result = query_job.result()

            # Emit metrics for each agent_id
            for row in result:
                agent_id = row.agent_id
                total_amount = float(row.total_amount or 0.0)
                row_count = int(row.row_count or 0)

                labels = {
                    "partner": str(partner_value),
                    "agent_id": str(agent_id),
                    "approved": approved_value,
                }

                # Emit amount metric
                emit_gauge_metric(
                    monitoring_client=self.monitoring_client,
                    project_id=self.run_project_id,
                    name="claims/pipeline/savings/vl_glosa_arvo",
                    value=total_amount,
                    labels=labels,
                    timestamp=source_timestamp,
                )

                # Emit count metric
                emit_gauge_metric(
                    monitoring_client=self.monitoring_client,
                    project_id=self.run_project_id,
                    name="claims/pipeline/savings/count",
                    value=float(row_count),
                    labels=labels,
                    timestamp=source_timestamp,
                )
        except NotFound:
            # If table doesn't exist, don't emit any metrics
            return
