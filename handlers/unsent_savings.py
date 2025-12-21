"""Handler for calculating and emitting unsent savings metrics."""

from datetime import datetime, timedelta

from google.cloud import bigquery, monitoring_v3

from handlers.base import Handler, HandlerBadRequestError
from metrics import emit_gauge_metric


class UnsentSavingsHandler(Handler):
    """
    Calculates and emits metrics for unsent savings.

    When the pipesv2_submission pipeline completes, this handler queries BigQuery
    to calculate the percentage of vl_glosa_arvo of submitted savings over the total
    of accepted savings within the last 2 days.

    To determine the time window, it uses the latest ingested_at timestamp from
    submitted_claims matching the submission_run_id, or falls back to source_timestamp
    minus 20 minutes if there are no submitted claims for the submission_run_id.

    Accepted savings are fetched from selected_savings_historical_table,
    internal_validation_output_table (with status = 'SUBMITTED_SUCCESS' or 'APPROVED'),
    and manual_validation_output_table (with status = 'SUBMITTED_SUCCESS' or 'APPROVED').

    Submitted savings are aggregated by status (SUBMISSION_ERROR, SUBMISSION_SUCCESS, RETRY)
    from the submitted_claims table.

    If there are no accepted savings (denominator is 0), emits one metric point with
    value 1.0 and status=SUBMISSION_SUCCESS.
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
        pipeline, which triggers the calculation of unsent savings metrics.

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
        Calculate unsent savings metrics and emit to Cloud Monitoring.

        Queries BigQuery to calculate the percentage of vl_glosa_arvo of submitted
        savings over the total of accepted savings within the last 2 days.

        Args:
            decoded_message: The decoded message dictionary containing pipeline completion data
        """
        payload = decoded_message.get("payload")

        if not payload:
            raise HandlerBadRequestError("No 'payload' found in event data.")

        variables = payload.get("variables", {})

        partner_value = variables.get("partner")
        selected_savings_historical_table = variables.get("selected_savings_historical_table")
        internal_validation_output_table = variables.get("internal_validation_output_table")
        manual_validation_output_table = variables.get("manual_validation_output_table")
        submitted_claims_output_table = variables.get("claims_submitted_output_table")
        submission_run_id = variables.get("submission_run_id")

        if not selected_savings_historical_table:
            raise HandlerBadRequestError("No 'selected_savings_historical_table' found in payload.")
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

        full_selected_savings_historical_table = ensure_full_table_ref(
            selected_savings_historical_table
        )
        full_internal_validation_output_table = ensure_full_table_ref(
            internal_validation_output_table
        )
        full_manual_validation_output_table = ensure_full_table_ref(manual_validation_output_table)
        full_submitted_claims_output_table = ensure_full_table_ref(submitted_claims_output_table)

        # Parse timestamp
        source_timestamp = datetime.fromisoformat(
            decoded_message["source_timestamp"].replace("Z", "+00:00")
        )

        # The fallback timestamp if there are no submitted claims for the submission_run_id
        twenty_minutes_ago_str = (source_timestamp - timedelta(minutes=20)).isoformat()

        # Query 1: Get the latest ingested_at timestamp for the submission_run_id
        # This determines the time window ceiling
        # Use FORMAT_TIMESTAMP to get ISO format string that TIMESTAMP() can parse
        time_window_query = f"""
        SELECT FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',
          COALESCE(MAX(ingested_at), TIMESTAMP '{twenty_minutes_ago_str}')
        ) as latest_ingested_at_str
        FROM `{full_submitted_claims_output_table}`
        WHERE submission_run_id = '{submission_run_id}'
        """
        time_window_job = self.bq_client.query(time_window_query)
        time_window_result = list(time_window_job.result())
        latest_ingested_at_str = (
            time_window_result[0].latest_ingested_at_str
            if time_window_result and time_window_result[0].latest_ingested_at_str
            else twenty_minutes_ago_str
        )

        # Query 2: Get the denominator - sum of vl_glosa_arvo from all accepted savings
        # within the time window (2 days lookback from latest_ingested_at)
        accepted_savings_query = f"""
        WITH date_range AS (
          SELECT TIMESTAMP_SUB(TIMESTAMP '{latest_ingested_at_str}', INTERVAL 2 DAY) as start_date,
            TIMESTAMP '{latest_ingested_at_str}' as end_date
        )
        SELECT COALESCE(SUM(vl_glosa_arvo), 0) as total_accepted
        FROM (
          SELECT vl_glosa_arvo, ingested_at
          FROM `{full_selected_savings_historical_table}`
          CROSS JOIN date_range AS dr
          WHERE ingested_at BETWEEN dr.start_date AND dr.end_date
          UNION ALL
          SELECT vl_glosa_arvo, ingested_at
          FROM `{full_internal_validation_output_table}`
          CROSS JOIN date_range AS dr
          WHERE ingested_at BETWEEN dr.start_date AND dr.end_date
            AND status IN ('SUBMITTED_SUCCESS', 'APPROVED')
          UNION ALL
          SELECT vl_glosa_arvo, ingested_at
          FROM `{full_manual_validation_output_table}`
          CROSS JOIN date_range AS dr
          WHERE ingested_at BETWEEN dr.start_date AND dr.end_date
            AND status IN ('SUBMITTED_SUCCESS', 'APPROVED')
        )
        """
        accepted_savings_job = self.bq_client.query(accepted_savings_query)
        accepted_savings_result = list(accepted_savings_job.result())
        total_accepted = (
            float(accepted_savings_result[0].total_accepted or 0.0)
            if accepted_savings_result
            else 0.0
        )

        # Query 3: Get the numerator - sum of vl_glosa_arvo from submitted_claims
        # grouped by status, within the time window
        submitted_savings_query = f"""
        WITH date_range AS (
          SELECT TIMESTAMP_SUB(TIMESTAMP '{latest_ingested_at_str}', INTERVAL 2 DAY) as start_date,
            TIMESTAMP '{latest_ingested_at_str}' as end_date
        )
        SELECT status, COALESCE(SUM(vl_glosa_arvo), 0) as total_submitted
        FROM `{full_submitted_claims_output_table}`
        CROSS JOIN date_range AS dr
        WHERE submission_run_id = '{submission_run_id}'
          AND ingested_at BETWEEN dr.start_date AND dr.end_date
        GROUP BY status
        """
        submitted_savings_job = self.bq_client.query(submitted_savings_query)
        submitted_savings_result = list(submitted_savings_job.result())

        # If denominator is 0, emit one metric point with value 1.0 and status=SUBMISSION_SUCCESS
        if total_accepted is None or total_accepted == 0.0:
            labels = {"partner": partner_value, "status": "SUBMISSION_SUCCESS"}
            emit_gauge_metric(
                monitoring_client=self.monitoring_client,
                project_id=self.run_project_id,
                name="claims/pipeline/savings/vl_glosa_arvo/sent_over_accepted_last_2_days",
                value=1.0,
                labels=labels,
                timestamp=source_timestamp,
            )
            return

        # Emit one metric per status
        # Ensure at least SUBMISSION_SUCCESS is present
        statuses_seen = {row.status for row in submitted_savings_result}
        if "SUBMISSION_SUCCESS" not in statuses_seen:
            statuses_seen.add("SUBMISSION_SUCCESS")

        for status in statuses_seen:
            # Find the total for this status
            total_for_status = 0.0
            for row in submitted_savings_result:
                if row.status == status:
                    total_for_status = float(row.total_submitted or 0.0)
                    break

            # Calculate percentage
            perc = total_for_status / total_accepted if total_accepted > 0 else 0.0

            labels = {"partner": partner_value, "status": status}
            emit_gauge_metric(
                monitoring_client=self.monitoring_client,
                project_id=self.run_project_id,
                name="claims/pipeline/savings/vl_glosa_arvo/sent_over_accepted_last_2_days",
                value=perc,
                labels=labels,
                timestamp=source_timestamp,
            )
