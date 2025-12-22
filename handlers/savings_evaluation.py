"""Handler for calculating and emitting savings metrics for evaluation pipeline.

This handler processes completion events from the pipesv2_evaluation pipeline
to compute aggregate metrics from savings results grouped by agent_id. It queries
BigQuery to aggregate savings values and emits metrics that track the total
value and count of savings per agent.
"""

from google.cloud import bigquery, monitoring_v3

from handlers.savings_base import SavingsBaseHandler


class SavingsEvaluationHandler(SavingsBaseHandler):
    """
    Calculates and emits metrics for savings grouped by agent_id from evaluation pipeline.

    When the pipesv2_evaluation pipeline completes, this handler queries BigQuery
    to aggregate savings values from the savings_output_table grouped by agent_id
    and emits two metrics per agent_id: amount and count. This enables monitoring
    of the financial impact of savings per agent during the evaluation pipeline execution.
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
        super().__init__(monitoring_client, bq_client, run_project_id, data_project_id)

    def match(self, decoded_message: dict) -> bool:
        """
        Determine if this handler should process the event.

        Matches events representing successful completion of the pipesv2_evaluation
        pipeline, which triggers the calculation of savings metrics per agent.

        Args:
            decoded_message: The decoded message dictionary to check

        Returns:
            True if this is a pipesv2_evaluation pipeline completion event,
            False otherwise
        """
        payload = decoded_message.get("payload", {})
        pipeline_uuid = payload.get("pipeline_uuid")
        pipeline_status = payload.get("status")

        return pipeline_uuid == "pipesv2_evaluation" and pipeline_status == "COMPLETED"

    def handle(self, decoded_message: dict) -> None:
        """
        Calculate savings metrics per agent_id and emit to Cloud Monitoring.

        Queries BigQuery to aggregate the total value and count of savings grouped
        by agent_id, then emits two metrics per agent_id: amount and count.
        If the table doesn't exist, no metrics are emitted.

        Args:
            decoded_message: The decoded message dictionary containing pipeline completion data

        Raises:
            HandlerBadRequestError: If the required input table variable
                (savings_output_table) or partner variable are not present
                in the event
        """
        self._handle_savings_metrics(
            decoded_message=decoded_message,
            pipeline_uuid="pipesv2_evaluation",
            savings_table_var="savings_output_table",
            approved_value="false",
        )
