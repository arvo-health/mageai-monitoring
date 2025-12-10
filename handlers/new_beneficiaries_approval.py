"""Handler for calculating and emitting new beneficiaries metrics for approval pipeline.

This handler processes completion events from the pipesv2_approval pipeline
to compute metrics about new beneficiaries (beneficiaries not present in a 3-month
rolling history).
"""

from google.cloud import bigquery, monitoring_v3

from handlers.new_beneficiaries_base import NewBeneficiariesBaseHandler


class NewBeneficiariesApprovalHandler(NewBeneficiariesBaseHandler):
    """
    Calculates and emits metrics for new beneficiaries from approval pipeline.

    When the pipesv2_approval pipeline completes, this handler queries BigQuery
    to identify beneficiaries in the batch that are not present in the historical
    data (3-month rolling window), then emits metrics representing the percentage
    of new beneficiaries. This enables monitoring of new beneficiary
    introduction during the approval pipeline execution.
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

        Matches events representing successful completion of the pipesv2_approval
        pipeline, which triggers the calculation of new beneficiaries metrics.

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
        Calculate new beneficiaries metrics and emit to Cloud Monitoring.

        Queries BigQuery to identify beneficiaries in the batch that are not present
        in the historical data (3-month rolling window), then emits metrics
        representing the percentage of new beneficiaries.

        Args:
            decoded_message: The decoded message dictionary containing pipeline completion data

        Raises:
            HandlerBadRequestError: If the required input table variables
                (processable_claims_input_table, unprocessable_claims_input_table,
                processable_claims_output_table, or unprocessable_claims_output_table)
                are not present in the event
        """
        self._handle_new_beneficiaries_metrics(
            decoded_message=decoded_message,
            batch_processable_table_var="processable_claims_input_table",
            batch_unprocessable_table_var="unprocessable_claims_input_table",
            historical_processable_table_var="processable_claims_output_table",
            historical_unprocessable_table_var="unprocessable_claims_output_table",
            approved_value="true",
        )
