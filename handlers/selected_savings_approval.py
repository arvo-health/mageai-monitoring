"""Handler for calculating and emitting savings metrics grouped by agent_id.

This handler processes completion events from the pipesv2_approval pipeline
to compute aggregate metrics from selected savings results. It queries
BigQuery to aggregate savings values grouped by agent_id and emits metrics
that track the total value of savings per agent.
"""

from datetime import datetime

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, monitoring_v3

from handlers.base import Handler, HandlerBadRequestError
from metrics import emit_gauge_metric


class SelectedSavingsApprovalHandler(Handler):
    """
    Calculates and emits metrics for savings grouped by agent_id.

    When the pipesv2_approval pipeline completes, this handler queries BigQuery
    to aggregate savings values from the selected_savings_input_table grouped by
    agent_id and emits one metric point per agent_id. This enables monitoring
    of the financial impact of savings per agent during the approval pipeline execution.
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
        pipeline, which triggers the calculation of savings metrics per agent.

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
        Calculate savings metrics per agent_id and emit to Cloud Monitoring.

        Queries BigQuery to aggregate the total value of savings grouped by agent_id,
        then emits one metric point per agent_id. Each metric has labels for partner,
        agent_id, and source. If the table doesn't exist, no metrics are emitted.

        Args:
            decoded_message: The decoded message dictionary containing pipeline completion data

        Raises:
            HandlerBadRequestError: If the required input table variable
                (selected_savings_input_table) or partner variable are not present
                in the event, or if the payload is missing
        """
        payload = decoded_message.get("payload")

        if not payload:
            raise HandlerBadRequestError("No 'payload' found in event data.")

        # Verify we're handling the right event (defensive check)
        if (
            payload.get("pipeline_uuid") != "pipesv2_approval"
            or payload.get("status") != "COMPLETED"
        ):
            return

        variables = payload.get("variables", {})

        selected_savings_table = variables.get("selected_savings_input_table")

        if not selected_savings_table:
            raise HandlerBadRequestError(
                "No variable 'selected_savings_input_table' found in payload."
            )

        # Partner label is required
        partner_value = variables.get("partner")
        if not partner_value:
            raise HandlerBadRequestError("Missing required 'partner' variable in payload.")

        # Ensure fully-qualified table path
        full_table = (
            selected_savings_table
            if "." in selected_savings_table
            else f"{self.data_project_id}.{selected_savings_table}"
        )

        # Parse timestamp
        source_timestamp = datetime.fromisoformat(
            decoded_message["source_timestamp"].replace("Z", "+00:00")
        )

        # Query BigQuery to get sum of vl_glosa_arvo grouped by agent_id
        # If table doesn't exist, don't emit any metrics
        try:
            # Check if table exists first to avoid hanging on query
            self.bq_client.get_table(full_table)
            # Table exists, so query it
            savings_query = f"""
            SELECT agent_id, COALESCE(SUM(vl_glosa_arvo), 0) AS total_vl_glosa_arvo
            FROM `{full_table}`
            GROUP BY agent_id
            """
            query_job = self.bq_client.query(savings_query)
            result = query_job.result()

            # Emit one metric point per agent_id
            for row in result:
                agent_id = row.agent_id
                total_vl_glosa_arvo = float(row.total_vl_glosa_arvo or 0.0)

                labels = {
                    "partner": str(partner_value),
                    "agent_id": str(agent_id),
                    "source": "selected_savings",
                }

                emit_gauge_metric(
                    monitoring_client=self.monitoring_client,
                    project_id=self.run_project_id,
                    name="claims/glosa/approved/vl_glosa_arvo",
                    value=total_vl_glosa_arvo,
                    labels=labels,
                    timestamp=source_timestamp,
                )
        except NotFound:
            # If table doesn't exist, don't emit any metrics
            return

