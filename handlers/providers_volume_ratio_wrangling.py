"""Handler for calculating and emitting providers volume ratio metrics for wrangling pipeline.

This handler processes completion events from the pipesv2_wrangling pipeline
to compute metrics about providers volume ratio.
"""

from google.cloud import bigquery, monitoring_v3

from handlers.providers_volume_ratio_base import ProvidersVolumeRatioBaseHandler


class ProvidersVolumeRatioWranglingHandler(ProvidersVolumeRatioBaseHandler):
    """
    Calculates and emits metrics for providers volume ratio from wrangling pipeline.

    When the pipesv2_wrangling pipeline completes, this handler queries BigQuery
    to calculate the ratio of the number of providers in the last 30 days,
    including the latest batch, to the number of providers in the previous 30 days period
    (D-60 to D-30). This enables monitoring of the volume of providers during
    the wrangling pipeline execution.
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
        pipeline, which triggers the calculation of providers volume ratio metrics.

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
        Calculate providers volume ratio metrics and emit to Cloud Monitoring.

        Queries BigQuery to calculate the ratio of the number of providers in the last 30 days,
        including the latest batch, to the number of providers in the previous 30 days period
        (D-60 to D-30).

        Args:
            decoded_message: The decoded message dictionary containing pipeline completion data

        Raises:
            HandlerBadRequestError: If the required input table variables
                (refined_processable_claims_output_table,
                refined_unprocessable_claims_output_table,
                refined_processable_claims_historical_table, or
                refined_unprocessable_claims_historical_table)
                are not present in the event
        """
        self._handle_providers_volume_ratio_metrics(
            decoded_message=decoded_message,
            batch_processable_table_var="refined_processable_claims_output_table",
            batch_unprocessable_table_var="refined_unprocessable_claims_output_table",
            historical_processable_table_var="refined_processable_claims_historical_table",
            historical_unprocessable_table_var="refined_unprocessable_claims_historical_table",
            approved_value="false",
        )
