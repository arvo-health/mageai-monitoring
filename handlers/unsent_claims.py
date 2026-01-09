"""Handler for calculating and emitting unsent claims metrics."""

from datetime import datetime, timedelta

from google.cloud import bigquery, monitoring_v3

from handlers.base import Handler, HandlerBadRequestError
from metrics import emit_gauge_metric


class UnsentClaimsHandler(Handler):
    """
    Calculates and emits metrics for unsent claims.

    When the pipesv2_submission pipeline completes, this handler queries BigQuery
    to list the claims in the processable_claims_output_table and unprocessable_claims_output_table
    that are not in the submitted_claims_output_table.

    It then emits two metrics representing the percentage of vl_pago and vl_info of sent claims
    over the total of ingested claims.

    To be considered unsent, besides being missing from the submitted_claims_output_table,
    the claim must have been ingested before the most recent submitted claim's `ingested_at`
    timestamp according to the submission_run_id, and be within the last 2 days.

    If there are no submitted claims for the submission_run_id, the handler will use the
    source_timestamp minus 20 minutes.
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
        pipeline, which triggers the calculation of unsent claims metrics.

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
        Calculate unsent claims metrics and emit to Cloud Monitoring.

        Queries BigQuery to list the claims in the processable_claims_output_table and
        unprocessable_claims_output_table that are not in the submitted_claims_output_table.
        It then emits a metric representing the total value of unsent claims (vl_pago and vl_info).

        Args:
            decoded_message: The decoded message dictionary containing pipeline completion data
        """
        payload = decoded_message.get("payload")

        if not payload:
            raise HandlerBadRequestError("No 'payload' found in event data.")

        variables = payload.get("variables", {})

        partner_value = variables.get("partner")
        processable_claims_historical_table = variables.get("processable_claims_historical_table")
        unprocessable_claims_historical_table = variables.get(
            "unprocessable_claims_historical_table"
        )
        internal_validation_output_table = variables.get("internal_validation_output_table")
        submitted_claims_output_table = variables.get("claims_submitted_output_table")
        submission_run_id = variables.get("submission_run_id")

        if not processable_claims_historical_table:
            raise HandlerBadRequestError(
                "No 'processable_claims_historical_table' found in payload."
            )
        if not unprocessable_claims_historical_table:
            raise HandlerBadRequestError(
                "No 'unprocessable_claims_historical_table' found in payload."
            )
        if not internal_validation_output_table:
            raise HandlerBadRequestError("No 'internal_validation_output_table' found in payload.")
        if not submitted_claims_output_table:
            raise HandlerBadRequestError("No 'submitted_claims_output_table' found in payload.")
        if not partner_value:
            raise HandlerBadRequestError("Missing required 'partner' variable in payload.")
        if not submission_run_id:
            raise HandlerBadRequestError(
                "Missing required 'submission_run_id' variable in payload."
            )

        # Ensure fully-qualified table paths
        def ensure_full_table_ref(table: str) -> str:
            return table if "." in table else f"{self.data_project_id}.{table}"

        full_processable_claims_historical_table = ensure_full_table_ref(
            processable_claims_historical_table
        )
        full_unprocessable_claims_historical_table = ensure_full_table_ref(
            unprocessable_claims_historical_table
        )
        full_internal_validation_output_table = ensure_full_table_ref(
            internal_validation_output_table
        )
        full_submitted_claims_output_table = ensure_full_table_ref(submitted_claims_output_table)

        # Parse timestamp
        source_timestamp = datetime.fromisoformat(
            decoded_message["source_timestamp"].replace("Z", "+00:00")
        )

        # The fallback timestamp if there are no submitted claims for the submission_run_id
        twenty_minutes_ago_str = (source_timestamp - timedelta(minutes=20)).isoformat()

        # Query to find claims that were ingested but not submitted
        # Check the claims in the processable_claims_historical_table and
        # unprocessable_claims_historical_table that are not in the submitted_claims_output_table
        # and also follow the time constraints and pending validation constraints.
        # Time constraints:
        # - The claims must have been ingested from before (or at the same time as) the most recent
        #   submitted claim's `ingested_at` timestamp according to the submission_run_id
        #   - If there are no submitted claims for the submission_run_id, the handler will use the
        #       source_timestamp minus 20 minutes as the fallback timestamp.
        # - The claims must have been ingested within the last 2 days, from the latest ingested_at
        #   timestamp (real or fallback)
        # The filter by submission_run_id and the fallback are important because if there are no
        # submitted claims associated with it, we don't want to get an older ingested_at timestamp
        # from another submission run, as it would give us a wrong time window.
        # Pending validation constraints:
        # - If a claim is pending validation, excluded or expired, it is ignored
        # - If a claim belongs to an invoice that has some claim pending validation, it is ignored
        # Ignored here means that the claim is not considered for the calculations. It will not be
        # counted as a claim that should have been sent.
        unsent_claims_query = f"""
        # Get the latest ingested_at timestamp for the submission_run_id
        WITH submitted_timestamps AS (
          SELECT
            COALESCE(MAX(ingested_at), TIMESTAMP '{twenty_minutes_ago_str}') as latest_ingested_at
          FROM `{full_submitted_claims_output_table}`
          WHERE submission_run_id = '{submission_run_id}'
        ),
        # Create the date range
        date_range AS (
          SELECT TIMESTAMP_SUB(latest_ingested_at, INTERVAL 2 DAY) as start_date,
            latest_ingested_at as end_date
          FROM submitted_timestamps
        ),
        # Get the ingested claims within the date range
        ingested_claims AS (
          SELECT id_arvo, vl_pago, vl_info
          FROM `{full_processable_claims_historical_table}`
          CROSS JOIN date_range AS dr
          WHERE ingested_at BETWEEN dr.start_date AND dr.end_date
          UNION DISTINCT
          SELECT id_arvo, vl_pago, vl_info
          FROM `{full_unprocessable_claims_historical_table}`
          CROSS JOIN date_range AS dr
          WHERE ingested_at BETWEEN dr.start_date AND dr.end_date
        ),
        # Get the ids of invoices that have claims pending validation
        pending_invoices AS (
          SELECT id_fatura
          FROM `{full_internal_validation_output_table}`
          CROSS JOIN date_range AS dr
          WHERE ingested_at BETWEEN dr.start_date AND dr.end_date
          GROUP BY id_fatura
          HAVING COUNT(CASE WHEN status = 'SENT_FOR_VALIDATION' THEN 1 END) > 0
        ),
        # Get the "non-sendable" claims (pending validation, expired or excluded)
        # Additionally, if the invoice is pending validation, all claims are non-sendable
        non_sendable_claims AS (
          SELECT id_arvo
          FROM `{full_internal_validation_output_table}`
          CROSS JOIN date_range AS dr
          WHERE ingested_at BETWEEN dr.start_date AND dr.end_date
            AND status IN ('SENT_FOR_VALIDATION', 'EXPIRED', 'EXCLUDED')
          UNION DISTINCT
          SELECT id_arvo
          FROM `{full_internal_validation_output_table}` iv
          INNER JOIN pending_invoices pi ON iv.id_fatura = pi.id_fatura
          CROSS JOIN date_range AS dr
          WHERE ingested_at BETWEEN dr.start_date AND dr.end_date
        ),
        # The ingested claims that are not in the non_sendable_claims are considered "sendable"
        sendable_claims AS (
          SELECT ic.id_arvo, ic.vl_pago, ic.vl_info
          FROM ingested_claims ic
          LEFT JOIN non_sendable_claims ns ON ic.id_arvo = ns.id_arvo
          WHERE ns.id_arvo IS NULL
        ),
        # Get the total vl_pago and vl_info of the sendable claims
        sendable_totals AS (
          SELECT sum(vl_pago) as total_pago, sum(vl_info) as total_info
          FROM sendable_claims
        ),
        # Get the total vl_pago and vl_info of the submitted claims by status
        totals_by_status AS (
          SELECT sub.status, SUM(send.vl_pago) as status_pago, SUM(send.vl_info) as status_info
          FROM `{full_submitted_claims_output_table}` sub
          INNER JOIN sendable_claims send ON sub.id_arvo = send.id_arvo
          GROUP BY sub.status
        ),
        # Get the statuses ensuring at least SUBMITTED_SUCCESS is present
        statuses AS (
          SELECT status FROM totals_by_status
          UNION DISTINCT
          SELECT 'SUBMITTED_SUCCESS' as status
        )
        # Calculate the percentage of vl_pago and vl_info for each status
        # If the total is 0 or NULL, we set the percentage to 1.0 (100%)
        # Otherwise, we calculate the ratio of the status values to the total values
        SELECT s.status,
          CASE IFNULL(st.total_pago, 0)
            WHEN 0 THEN 1.0
            ELSE COALESCE(SAFE_DIVIDE(tbs.status_pago, st.total_pago), 0)
          END AS perc_pago,
          CASE IFNULL(st.total_info, 0)
            WHEN 0 THEN 1.0
            ELSE COALESCE(SAFE_DIVIDE(tbs.status_info, st.total_info), 0)
          END AS perc_info
        FROM statuses s
        LEFT JOIN totals_by_status tbs ON s.status = tbs.status
        CROSS JOIN sendable_totals st
        """
        query_job = self.bq_client.query(unsent_claims_query)
        result = query_job.result()

        for row in result:
            status = row.status
            perc_pago = float(row.perc_pago or 0.0)
            perc_info = float(row.perc_info or 0.0)

            labels = {"partner": partner_value, "status": status}
            emit_gauge_metric(
                monitoring_client=self.monitoring_client,
                project_id=self.run_project_id,
                name="claims/pipeline/claims/vl_pago/sent_over_recv_last_2_days",
                value=perc_pago,
                labels=labels,
                timestamp=source_timestamp,
            )
            emit_gauge_metric(
                monitoring_client=self.monitoring_client,
                project_id=self.run_project_id,
                name="claims/pipeline/claims/vl_info/sent_over_recv_last_2_days",
                value=perc_info,
                labels=labels,
                timestamp=source_timestamp,
            )
