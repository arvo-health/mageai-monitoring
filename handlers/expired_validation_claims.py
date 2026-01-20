"""Handler for calculating and emitting expired validation claims metrics."""

from datetime import datetime

from google.cloud import bigquery, monitoring_v3
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from handlers.base import Handler, HandlerBadRequestError
from metrics import emit_gauge_metric


class ExpiredValidationClaimsHandler(Handler):
    """
    Calculates and emits metrics for expired validation claims.

    When the pipesv2_release pipeline completes, this handler queries BigQuery
    to calculate the sum of vl_glosa_arvo for expired claims from both internal
    and manual validation tables.

    The query uses a 2-day partition filter on ingested_at and a 30-minute row filter
    on updated_at to optimize costs and focus on recently updated items.

    Emits metric:
    - claims/pipeline/validation/vl_glosa_arvo/expired/total (sum of internal + manual)
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

        Matches events representing successful completion of the pipesv2_release
        pipeline, which triggers the calculation of expired validation claims metrics.

        Args:
            decoded_message: The decoded message dictionary to check

        Returns:
            True if this is a pipesv2_release pipeline completion event,
            False otherwise
        """
        payload = decoded_message.get("payload")

        if not payload:
            return False

        pipeline_uuid = payload.get("pipeline_uuid")
        pipeline_status = payload.get("status")

        return pipeline_uuid == "pipesv2_release" and pipeline_status == "COMPLETED"

    def handle(self, decoded_message: dict) -> None:
        """
        Calculate expired validation claims metrics and emit to Cloud Monitoring.

        Queries BigQuery to calculate the sum of vl_glosa_arvo for expired claims
        from both internal and manual validation tables, then emits metrics for each.

        Args:
            decoded_message: The decoded message dictionary containing pipeline completion data
        """
        payload = decoded_message.get("payload")

        if not payload:
            raise HandlerBadRequestError("No 'payload' found in event data.")

        variables = payload.get("variables", {})

        partner_value = variables.get("partner")
        internal_validation = variables.get("internal_validation", {})
        manual_validation = variables.get("manual_validation", {})
        internal_validation_table = internal_validation.get(
            "internal_validation_claims_input_table"
        )
        manual_validation_table = manual_validation.get("manual_validation_claims_input_table")

        if not partner_value:
            raise HandlerBadRequestError("Missing required 'partner' variable in payload.")
        if not internal_validation_table:
            raise HandlerBadRequestError(
                "No 'internal_validation_claims_input_table' found in payload."
            )
        if not manual_validation_table:
            raise HandlerBadRequestError(
                "No 'manual_validation_claims_input_table' found in payload."
            )

        # Ensure fully-qualified table paths
        def ensure_full_table_ref(table: str) -> str:
            return table if "." in table else f"{self.data_project_id}.{table}"

        full_internal_validation_table = ensure_full_table_ref(internal_validation_table)
        full_manual_validation_table = ensure_full_table_ref(manual_validation_table)

        # Parse timestamp
        source_timestamp = datetime.fromisoformat(
            decoded_message["source_timestamp"].replace("Z", "+00:00")
        )

        # Query both tables and emit combined metric
        internal_total = self._query_expired_sum(full_internal_validation_table, source_timestamp)
        manual_total = self._query_expired_sum(full_manual_validation_table, source_timestamp)

        combined_total = internal_total + manual_total
        emit_gauge_metric(
            monitoring_client=self.monitoring_client,
            project_id=self.run_project_id,
            name="claims/pipeline/validation/vl_glosa_arvo/expired/total",
            value=combined_total,
            labels={"partner": partner_value},
            timestamp=source_timestamp,
        )

    def _query_expired_sum(self, table_name: str, source_timestamp: datetime) -> float:
        """
        Query the sum of vl_glosa_arvo for expired claims.

        Uses a 2-day partition filter on ingested_at and a 30-minute row filter
        on updated_at to optimize query costs and focus on recently updated items.

        Args:
            table_name: Fully-qualified BigQuery table name
            source_timestamp: The source timestamp from the event

        Returns:
            The sum of vl_glosa_arvo for expired claims, or 0 if none found
        """
        query = f"""
        SELECT COALESCE(SUM(vl_glosa_arvo), 0) as total_vl_glosa_arvo
        FROM `{table_name}`
        WHERE
            ingested_at >= TIMESTAMP_SUB(@source_timestamp, INTERVAL 2 DAY)
            AND ingested_at <= @source_timestamp
            AND updated_at >= DATETIME_SUB(DATETIME(@source_timestamp), INTERVAL 30 MINUTE)
            AND updated_at <= DATETIME(@source_timestamp)
            AND status = 'EXPIRED'
        """

        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("source_timestamp", "TIMESTAMP", source_timestamp),
            ]
        )

        query_job = self.bq_client.query(query, job_config=job_config)
        result = query_job.result()
        row = next(result, None)

        if row:
            return float(row.total_vl_glosa_arvo or 0.0)
        return 0.0
