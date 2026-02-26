"""Handler for calculating and emitting SHARED_ID_FATURA completeness metrics.

This handler checks that 100% of savings in the validation pipeline
(internal + manual validation) with filtered_reason = 'SHARED_ID_FATURA'
were submitted to the partner.
"""

from datetime import UTC, datetime, timedelta

from google.cloud import bigquery, monitoring_v3
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from handlers.base import Handler, HandlerBadRequestError
from metrics import emit_gauge_metric


class SharedIdFaturaCompletenessHandler(Handler):
    """
    Calculates and emits completeness metrics for SHARED_ID_FATURA savings.

    When the pipesv2_submission pipeline completes, this handler queries BigQuery
    to verify that 100% of savings in internal_validation and manual_validation tables
    with filtered_reason = 'SHARED_ID_FATURA' (and status APPROVED or SUBMITTED_SUCCESS)
    were submitted to the partner within the last 2 days.

    SHARED_ID_FATURA items are savings that were flagged because they share an invoice
    (id_fatura) with another saving that was filtered for a specific reason. The business
    rule is that 100% of these items must be submitted once they are approved.

    To determine the time window, it uses the latest ingested_at timestamp from
    submitted_claims matching the submission_run_id, or falls back to source_timestamp
    minus 20 minutes if there are no submitted claims for the submission_run_id.

    If there are no SHARED_ID_FATURA items in the validation pipeline (denominator is 0),
    emits value 1.0 (completeness is trivially 100%).

    Emits:
        claims/pipeline/savings/vl_glosa_arvo/shared_id_fatura_sent_over_validation
        Value: 0.0-1.0 (1.0 = 100% complete, i.e., all SHARED_ID_FATURA items submitted)
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

        Matches events representing successful completion of the pipesv2_submission
        pipeline.

        Args:
            decoded_message: The decoded message dictionary to check

        Returns:
            True if this is a pipesv2_submission pipeline completion event,
            False otherwise
        """
        payload = decoded_message.get("payload")

        if not payload:
            return False

        pipeline_uuid = payload.get("pipeline_uuid")
        pipeline_status = payload.get("status")

        return pipeline_uuid == "pipesv2_submission" and pipeline_status == "COMPLETED"

    def handle(self, decoded_message: dict) -> None:
        """
        Calculate SHARED_ID_FATURA completeness metrics and emit to Cloud Monitoring.

        Queries BigQuery to calculate what percentage of SHARED_ID_FATURA savings
        in internal + manual validation were submitted within the last 2 days.
        A value of 1.0 means 100% of SHARED_ID_FATURA items were submitted (healthy).
        A value below 1.0 means some items were missed and requires investigation.

        Args:
            decoded_message: The decoded message dictionary containing pipeline completion data

        Raises:
            HandlerBadRequestError: If the required input table variables are not present
                in the event, or if the payload is missing
        """
        payload = decoded_message.get("payload")

        if not payload:
            raise HandlerBadRequestError("No 'payload' found in event data.")

        variables = payload.get("variables", {})

        partner_value = variables.get("partner")
        internal_validation_output_table = variables.get("internal_validation_output_table")
        manual_validation_output_table = variables.get("manual_validation_output_table")
        submitted_claims_output_table = variables.get("claims_submitted_output_table")
        submission_run_id = variables.get("submission_run_id")

        if not internal_validation_output_table:
            raise HandlerBadRequestError("No 'internal_validation_output_table' found in payload.")
        if not manual_validation_output_table:
            raise HandlerBadRequestError("No 'manual_validation_output_table' found in payload.")
        if not submitted_claims_output_table:
            raise HandlerBadRequestError("No 'claims_submitted_output_table' found in payload.")
        if not partner_value:
            raise HandlerBadRequestError("Missing required 'partner' variable in payload.")
        if not submission_run_id:
            raise HandlerBadRequestError(
                "Missing required 'submission_run_id' variable in payload."
            )

        # Ensure fully-qualified table paths
        def ensure_full_table_ref(table: str) -> str:
            return table if "." in table else f"{self.data_project_id}.{table}"

        full_internal = ensure_full_table_ref(internal_validation_output_table)
        full_manual = ensure_full_table_ref(manual_validation_output_table)
        full_submitted = ensure_full_table_ref(submitted_claims_output_table)

        # Parse timestamp
        source_timestamp = datetime.fromisoformat(
            decoded_message["source_timestamp"].replace("Z", "+00:00")
        )

        # The fallback timestamp if there are no submitted claims for the submission_run_id
        twenty_minutes_ago = source_timestamp - timedelta(minutes=20)
        if twenty_minutes_ago.tzinfo is None:
            twenty_minutes_ago = twenty_minutes_ago.replace(tzinfo=UTC)

        # Query: find all SHARED_ID_FATURA items in internal + manual validation
        # with status APPROVED or SUBMITTED_SUCCESS (i.e., eligible to be submitted).
        # Then calculate how many of those were actually submitted.
        query = f"""
        WITH submitted_timestamps AS (
          SELECT COALESCE(MAX(ingested_at), @fallback_timestamp) AS latest_ingested_at
          FROM `{full_submitted}`
          WHERE submission_run_id = @submission_run_id
        ),
        date_range AS (
          SELECT TIMESTAMP_SUB(latest_ingested_at, INTERVAL 2 DAY) AS start_date,
                 latest_ingested_at AS end_date
          FROM submitted_timestamps
        ),
        shared_id_fatura_savings AS (
          SELECT id_arvo, vl_glosa_arvo
          FROM `{full_internal}`
          CROSS JOIN date_range AS dr
          WHERE ingested_at BETWEEN dr.start_date AND dr.end_date
            AND filtered_reason = 'SHARED_ID_FATURA'
            AND status IN ('SUBMITTED_SUCCESS', 'APPROVED')
          UNION ALL
          SELECT id_arvo, vl_glosa_arvo
          FROM `{full_manual}`
          CROSS JOIN date_range AS dr
          WHERE ingested_at BETWEEN dr.start_date AND dr.end_date
            AND filtered_reason = 'SHARED_ID_FATURA'
            AND status IN ('SUBMITTED_SUCCESS', 'APPROVED')
        ),
        total_shared AS (
          SELECT COALESCE(SUM(vl_glosa_arvo), 0) AS total
          FROM shared_id_fatura_savings
        ),
        shared_ids AS (
          SELECT DISTINCT id_arvo
          FROM shared_id_fatura_savings
        ),
        submitted_shared AS (
          SELECT COALESCE(SUM(s.vl_glosa_arvo), 0) AS submitted
          FROM `{full_submitted}` s
          INNER JOIN shared_ids si ON s.id_arvo = si.id_arvo
        )
        SELECT
          ts.total,
          ss.submitted
        FROM total_shared ts
        CROSS JOIN submitted_shared ss
        """

        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("submission_run_id", "STRING", submission_run_id),
                ScalarQueryParameter("fallback_timestamp", "TIMESTAMP", twenty_minutes_ago),
            ]
        )
        job = self.bq_client.query(query, job_config=job_config)
        result = list(job.result())

        total = float(result[0].total or 0.0) if result else 0.0
        submitted = float(result[0].submitted or 0.0) if result else 0.0

        # Completeness: 1.0 = 100% submitted (healthy). If no SHARED_ID_FATURA items, it's 1.0.
        completeness = submitted / total if total > 0 else 1.0

        labels = {"partner": partner_value}
        emit_gauge_metric(
            monitoring_client=self.monitoring_client,
            project_id=self.run_project_id,
            name="claims/pipeline/savings/vl_glosa_arvo/shared_id_fatura_sent_over_validation",
            value=completeness,
            labels=labels,
            timestamp=source_timestamp,
        )
