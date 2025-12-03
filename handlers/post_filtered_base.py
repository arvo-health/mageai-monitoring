"""Base handler for post-filtered handlers.

This module contains the base class for handlers that calculate and emit
post-processing filter metrics from pipeline completion events.
"""

from datetime import datetime

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, monitoring_v3

from handlers.base import Handler, HandlerBadRequestError
from metrics import emit_gauge_metric


class PostFilteredBaseHandler(Handler):
    """
    Base handler for calculating and emitting post-processing filter metrics.

    This base class provides common functionality for handlers that process
    pipeline completion events to compute aggregate metrics from post-processing
    filter results. Subclasses should implement the `match` method and call
    `_handle_post_filtered_metrics` with pipeline-specific configuration.
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

    def _handle_post_filtered_metrics(
        self,
        decoded_message: dict,
        pipeline_uuid: str,
        excluded_table_var: str,
        savings_table_var: str,
        approved_value: str,
    ) -> None:
        """
        Calculate post-processing filter metrics and emit to Cloud Monitoring.

        Queries BigQuery to aggregate the total value of savings filtered during
        the post-processing stage, then emits metrics representing both the total
        and relative values. The relative metric is calculated as the ratio of
        excluded savings value to the total value of all savings.

        If the excluded table doesn't exist, assumes its sum is 0.
        The savings table must exist; if it doesn't, the exception will be raised.

        Args:
            decoded_message: The decoded message dictionary containing pipeline completion data
            pipeline_uuid: The pipeline UUID to verify (for defensive check)
            excluded_table_var: Variable name for the excluded savings table
            savings_table_var: Variable name for the savings table
            approved_value: Value for the "approved" label ("true" or "false")

        Raises:
            HandlerBadRequestError: If the required input table variables are not present
                in the event, or if the payload is missing
            NotFound: If the savings table doesn't exist
        """
        payload = decoded_message.get("payload")

        if not payload:
            raise HandlerBadRequestError("No 'payload' found in event data.")

        variables = payload.get("variables", {})

        # Verify we're still handling the right event (defensive check)
        if payload.get("pipeline_uuid") != pipeline_uuid or payload.get("status") != "COMPLETED":
            return

        excluded_table = variables.get(excluded_table_var)
        savings_table = variables.get(savings_table_var)

        if not excluded_table:
            raise HandlerBadRequestError(f"No variable '{excluded_table_var}' found in payload.")

        if not savings_table:
            raise HandlerBadRequestError(f"No variable '{savings_table_var}' found in payload.")

        # Ensure fully-qualified table paths
        def ensure_full_table_ref(table: str) -> str:
            return table if "." in table else f"{self.data_project_id}.{table}"

        full_excluded_table = ensure_full_table_ref(excluded_table)
        full_savings_table = ensure_full_table_ref(savings_table)

        # Check if excluded table exists and query it separately
        # If excluded table doesn't exist, assume sum is 0
        total_vl_glosa_arvo = 0.0
        try:
            # Check if table exists first to avoid hanging on query
            self.bq_client.get_table(full_excluded_table)
            # Table exists, so query it
            excluded_query = f"""
            SELECT COALESCE(SUM(vl_glosa_arvo), 0) AS total_vl_glosa_arvo
            FROM `{full_excluded_table}`
            """
            query_job = self.bq_client.query(excluded_query)
            result = query_job.result()
            row = next(result, None)
            if row:
                total_vl_glosa_arvo = float(row.total_vl_glosa_arvo or 0.0)
        except NotFound:
            # If table doesn't exist, assume sum is 0
            total_vl_glosa_arvo = 0.0

        # Query savings table - must exist, so let exception be raised if it doesn't
        savings_query = f"""
        SELECT COALESCE(SUM(vl_glosa_arvo), 0) AS sum_savings_vl_glosa_arvo
        FROM `{full_savings_table}`
        """
        query_job = self.bq_client.query(savings_query)
        result = query_job.result()
        row = next(result, None)
        sum_savings_vl_glosa_arvo = float(row.sum_savings_vl_glosa_arvo or 0.0)

        # Calculate relative value
        if sum_savings_vl_glosa_arvo > 0:
            relative_vl_glosa_arvo = total_vl_glosa_arvo / sum_savings_vl_glosa_arvo
        else:
            relative_vl_glosa_arvo = 0.0

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

        # Emit metric representing total value of savings filtered in post-processing
        emit_gauge_metric(
            monitoring_client=self.monitoring_client,
            project_id=self.run_project_id,
            name="claims/pipeline/filtered_post/vl_glosa_arvo/total",
            value=total_vl_glosa_arvo,
            labels=labels,
            timestamp=source_timestamp,
        )

        # Emit metric representing relative value of savings filtered in post-processing
        emit_gauge_metric(
            monitoring_client=self.monitoring_client,
            project_id=self.run_project_id,
            name="claims/pipeline/filtered_post/vl_glosa_arvo/relative",
            value=relative_vl_glosa_arvo,
            labels=labels,
            timestamp=source_timestamp,
        )
