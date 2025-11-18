"""Handler for calculating and emitting pre-processing filter metrics for wrangling pipeline.

This handler processes completion events from the pipesv2_wrangling pipeline
to compute aggregate metrics from pre-processing filter results. It queries
BigQuery to aggregate claim values and emits metrics that track the total
value of claims that were filtered during the pre-processing stage.
"""

from google.cloud import bigquery, monitoring_v3

from handlers.pre_filtered_base import PreFilteredBaseHandler


class PreFilteredWranglingHandler(PreFilteredBaseHandler):
    """
    Calculates and emits metrics for pre-processing filter results from wrangling pipeline.

    When the pipesv2_wrangling pipeline completes, this handler queries BigQuery
    to aggregate claim values from the pre-processing filter stage and emits
    two metrics: one representing the total value of filtered claims, and another
    representing the relative value (ratio of filtered claims to processable claims).
    This enables monitoring of the financial impact of pre-processing filters applied
    during the wrangling pipeline execution.
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

        Matches events representing successful completion of the pipesv2_wrangling
        pipeline, which triggers the calculation of pre-processing filter metrics.

        Args:
            decoded_message: The decoded message dictionary to check

        Returns:
            True if this is a pipesv2_wrangling pipeline completion event,
            False otherwise
        """
        payload = decoded_message.get("payload")

        if not payload:
            return False

        pipeline_uuid = payload.get("pipeline_uuid")
        pipeline_status = payload.get("status")

        return pipeline_uuid == "pipesv2_wrangling" and pipeline_status == "COMPLETED"

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
                (refined_unprocessable_claims_output_table or
                refined_processable_claims_output_table)
                are not present in the event
        """
        self._handle_pre_filtered_metrics(
            decoded_message=decoded_message,
            pipeline_uuid="pipesv2_wrangling",
            unprocessable_table_var="refined_unprocessable_claims_output_table",
            processable_table_var="refined_processable_claims_output_table",
            approved_value="false",
        )
