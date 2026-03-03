"""Handler for calculating and emitting sent validation savings metrics.

This handler calculates the percentage of savings that went through the
validation pipeline (internal + manual validation) and were successfully
submitted, excluding items with filtered_reason = 'SHARED_ID_FATURA'.
"""

from datetime import UTC, datetime, timedelta

from google.cloud import bigquery, monitoring_v3
from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

from handlers.base import Handler, HandlerBadRequestError
from metrics import emit_gauge_metric


class SentValidationSavingsHandler(Handler):
    """
    Calculates and emits metrics for the percentage of validation savings submitted.

    When the pipesv2_submission pipeline completes, this handler queries BigQuery
    to calculate the percentage of vl_glosa_arvo of submitted savings over the total
    of savings that went through the validation pipeline (internal_validation +
    manual_validation) within the last 2 days, excluding items with
    filtered_reason = 'SHARED_ID_FATURA'.

    This metric complements the existing UnsentSavingsHandler (which includes
    selected_savings in the denominator). This handler focuses specifically on
    the validation pipeline items, allowing monitoring of the submission rate
    for savings that required human review.

    To determine the time window, it uses the latest ingested_at timestamp from
    submitted_claims matching the submission_run_id, or falls back to source_timestamp
    minus 20 minutes if there are no submitted claims for the submission_run_id.

    Items with filtered_reason = 'SHARED_ID_FATURA' are excluded from both the
    numerator and denominator (tracked separately by SharedIdFaturaCompletenessHandler).

    Denominator = all items in internal + manual validation that were analyzed, meaning
    status NOT IN ('SENT_FOR_VALIDATION', 'NOT_SENT_FOR_VALIDATION'). This includes:
    APPROVED, DENIED, EXCLUDED, EXPIRED, RELEASE_TRAIN, SUBMITTED_SUCCESS, SUBMITTED_ERROR.
    Items with SENT_FOR_VALIDATION (still pending review) and NOT_SENT_FOR_VALIDATION
    (filtered before reaching the validator) are excluded from the denominator.

    If there are no eligible validation savings (denominator is 0), emits one metric
    point with value 1.0 and status=SUBMITTED_SUCCESS.
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
        Calculate sent validation savings metrics and emit to Cloud Monitoring.

        Queries BigQuery to calculate the percentage of vl_glosa_arvo of submitted
        savings over the total of validation savings (internal + manual validation,
        excluding SHARED_ID_FATURA) within the last 2 days.

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

        # Query: denominator = internal + manual validation items that were analyzed, i.e.
        # status NOT IN ('SENT_FOR_VALIDATION', 'NOT_SENT_FOR_VALIDATION'), excluding
        # SHARED_ID_FATURA items (tracked separately). This covers: APPROVED, DENIED,
        # EXCLUDED, EXPIRED, RELEASE_TRAIN, SUBMITTED_SUCCESS, SUBMITTED_ERROR.
        # Numerator = submitted items that match those id_arvos, grouped by status.
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
        validation_savings AS (
          SELECT id_arvo, vl_glosa_arvo
          FROM `{full_internal}`
          CROSS JOIN date_range AS dr
          WHERE ingested_at BETWEEN dr.start_date AND dr.end_date
            AND status NOT IN ('SENT_FOR_VALIDATION', 'NOT_SENT_FOR_VALIDATION')
            AND (filtered_reason IS NULL OR filtered_reason != 'SHARED_ID_FATURA')
          UNION ALL
          SELECT id_arvo, vl_glosa_arvo
          FROM `{full_manual}`
          CROSS JOIN date_range AS dr
          WHERE ingested_at BETWEEN dr.start_date AND dr.end_date
            AND status NOT IN ('SENT_FOR_VALIDATION', 'NOT_SENT_FOR_VALIDATION')
            AND (filtered_reason IS NULL OR filtered_reason != 'SHARED_ID_FATURA')
        ),
        total_validation AS (
          SELECT COALESCE(SUM(vl_glosa_arvo), 0) AS total
          FROM validation_savings
        ),
        validation_ids AS (
          SELECT DISTINCT id_arvo
          FROM validation_savings
        ),
        submitted_by_status AS (
          SELECT s.status, COALESCE(SUM(s.vl_glosa_arvo), 0) AS total_submitted
          FROM `{full_submitted}` s
          INNER JOIN validation_ids vi ON s.id_arvo = vi.id_arvo
          GROUP BY s.status
        ),
        statuses AS (
          SELECT status FROM submitted_by_status
          UNION DISTINCT
          SELECT 'SUBMITTED_SUCCESS' AS status
        )
        SELECT
          s.status,
          COALESCE(ss.total_submitted, 0) AS total_submitted,
          tv.total AS total_validation
        FROM statuses s
        CROSS JOIN total_validation tv
        LEFT JOIN submitted_by_status ss ON s.status = ss.status
        """

        job_config = QueryJobConfig(
            query_parameters=[
                ScalarQueryParameter("submission_run_id", "STRING", submission_run_id),
                ScalarQueryParameter("fallback_timestamp", "TIMESTAMP", twenty_minutes_ago),
            ]
        )
        job = self.bq_client.query(query, job_config=job_config)
        result = list(job.result())

        total_validation = float(result[0].total_validation or 0.0) if result else 0.0
        submitted_by_status = {row.status: float(row.total_submitted or 0.0) for row in result}

        has_submitted_values = any(v > 0.0 for v in submitted_by_status.values())

        # If denominator is 0 and there are no submitted values,
        # emit only SUBMITTED_SUCCESS with value 1.0
        if total_validation == 0.0 and not has_submitted_values:
            labels = {"partner": partner_value, "status": "SUBMITTED_SUCCESS"}
            emit_gauge_metric(
                monitoring_client=self.monitoring_client,
                project_id=self.run_project_id,
                name="claims/pipeline/savings/vl_glosa_arvo/sent_over_validation_last_2_days",
                value=1.0,
                labels=labels,
                timestamp=source_timestamp,
            )
            return

        # Emit one metric per status
        statuses = set(submitted_by_status.keys())
        if "SUBMITTED_SUCCESS" not in statuses:
            statuses.add("SUBMITTED_SUCCESS")

        for status in statuses:
            total_for_status = submitted_by_status.get(status, 0.0)
            perc = total_for_status / total_validation if total_validation > 0 else 0.0

            labels = {"partner": partner_value, "status": status}
            emit_gauge_metric(
                monitoring_client=self.monitoring_client,
                project_id=self.run_project_id,
                name="claims/pipeline/savings/vl_glosa_arvo/sent_over_validation_last_2_days",
                value=perc,
                labels=labels,
                timestamp=source_timestamp,
            )
