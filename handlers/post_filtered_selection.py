"""Handler for calculating and emitting post-processing filter metrics for selection pipeline.

This handler processes completion events from the pipesv2_selection pipeline
to compute aggregate metrics from post-processing filter results. It queries
BigQuery to aggregate savings values and emits metrics that track the total
value of savings that were filtered during the post-processing stage.
"""

from google.cloud import bigquery, monitoring_v3

from handlers.post_filtered_base import PostFilteredBaseHandler


class PostFilteredSelectionHandler(PostFilteredBaseHandler):
    """
    Calculates and emits metrics for post-processing filter results from selection pipeline.

    When the pipesv2_selection pipeline completes, this handler queries BigQuery
    to aggregate savings values from the post-processing filter stage and emits
    two metrics: one representing the total value of filtered savings, and another
    representing the relative value (ratio of excluded savings to total savings).
    This enables monitoring of the financial impact of post-processing filters applied
    during the selection pipeline execution.
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

        Matches events representing successful completion of the pipesv2_selection
        pipeline, which triggers the calculation of post-processing filter metrics.

        Args:
            decoded_message: The decoded message dictionary to check

        Returns:
            True if this is a pipesv2_selection pipeline completion event,
            False otherwise
        """
        payload = decoded_message.get("payload")

        if not payload:
            return False

        pipeline_uuid = payload.get("pipeline_uuid")
        pipeline_status = payload.get("status")

        return pipeline_uuid == "pipesv2_selection" and pipeline_status == "COMPLETED"

    def handle(self, decoded_message: dict) -> None:
        """
        Calculate post-processing filter metrics and emit to Cloud Monitoring.

        Queries BigQuery to aggregate the total value of savings filtered during
        the post-processing stage, then emits metrics representing both the total
        and relative values. The relative metric is calculated as the ratio of
        excluded savings value to the total value of all savings.

        Args:
            decoded_message: The decoded message dictionary containing pipeline completion data

        Raises:
            HandlerBadRequestError: If the required input table variables
                (excluded_savings_output_table or savings_input_table)
                are not present in the event
        """
        self._handle_post_filtered_metrics(
            decoded_message=decoded_message,
            pipeline_uuid="pipesv2_selection",
            excluded_table_var="excluded_savings_output_table",
            savings_table_var="savings_input_table",
            approved_value="false",
        )
