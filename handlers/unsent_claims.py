"""Handler for calculating and emitting unsent claims metrics."""

from datetime import datetime, timedelta

from handlers.base import Handler, HandlerBadRequestError
from google.cloud import bigquery, monitoring_v3
from metrics import emit_gauge_metric


class UnsentClaimsHandler(Handler):
    """
    Calculates and emits metrics for unsent claims.

    When the pipesv2_submission pipeline completes, this handler queries BigQuery
    to list the claims in the processable_claims_output_table and unprocessable_claims_output_table
    that are not in the submitted_claims_output_table.

    It then emits two metrics representing the sum of vl_pago and vl_info of unsent claims.

    To be considered unsent, besides being missing from the submitted_claims_output_table, the claim must
    have been ingested before the most recent submitted claim's `ingested_at` timestamp according to the submission_run_id,
    and be within the last 2 days.

    If there are no submitted claims for the submission_run_id, the handler will use the source timestamp minus 20 minutes.
    """
    def __init__(self, monitoring_client: monitoring_v3.MetricServiceClient, bq_client: bigquery.Client, run_project_id: str, data_project_id: str):
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

        Queries BigQuery to list the claims in the processable_claims_output_table and unprocessable_claims_output_table that
        are not in the submitted_claims_output_table.
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
        unprocessable_claims_historical_table = variables.get("unprocessable_claims_historical_table")
        submitted_claims_output_table = variables.get("claims_submitted_output_table")
        submission_run_id = variables.get("submission_run_id")

        if not processable_claims_historical_table:
            raise HandlerBadRequestError("No 'processable_claims_output_table' found in payload.")
        if not unprocessable_claims_historical_table:
            raise HandlerBadRequestError("No 'unprocessable_claims_output_table' found in payload.")
        if not submitted_claims_output_table:
            raise HandlerBadRequestError("No 'submitted_claims_output_table' found in payload.")
        if not partner_value:
            raise HandlerBadRequestError("Missing required 'partner' variable in payload.")
        if not submission_run_id:
            raise HandlerBadRequestError("Missing required 'submission_run_id' variable in payload.")

        # Ensure fully-qualified table paths
        def ensure_full_table_ref(table: str) -> str:
            return table if "." in table else f"{self.data_project_id}.{table}"

        full_processable_claims_historical_table = ensure_full_table_ref(processable_claims_historical_table)
        full_unprocessable_claims_historical_table = ensure_full_table_ref(unprocessable_claims_historical_table)
        full_submitted_claims_output_table = ensure_full_table_ref(submitted_claims_output_table)

        # Parse timestamp
        source_timestamp = datetime.fromisoformat(
            decoded_message["source_timestamp"].replace("Z", "+00:00")
        )

        # The fallback timestamp if there are no submitted claims for the submission_run_id
        twenty_minutes_ago_str = (source_timestamp - timedelta(minutes=20)).isoformat()

        # Query to find claims that were ingested but not submitted
        # List the claims in the processable_claims_output_table and unprocessable_claims_output_table that
        # are not in the submitted_claims_output_table and also follow the time constraints.
        unsent_claims_query = f"""
        WITH submitted AS (
          SELECT COALESCE(MAX(ingested_at), TIMESTAMP '{twenty_minutes_ago_str}') as latest_ingested_at
          FROM `{full_submitted_claims_output_table}`
          WHERE submission_run_id = '{submission_run_id}'
        ),
        date_range AS (
          SELECT TIMESTAMP_SUB(latest_ingested_at, INTERVAL 2 DAY) as start_date, latest_ingested_at as end_date
          FROM submitted
        ),
        ingested_claims AS (
          SELECT id_arvo, vl_pago, vl_info
          FROM `{full_processable_claims_historical_table}`
          WHERE ingested_at BETWEEN (SELECT start_date FROM date_range) AND (SELECT end_date FROM date_range)
          UNION DISTINCT
          SELECT id_arvo, vl_pago, vl_info
          FROM `{full_unprocessable_claims_historical_table}`
          WHERE ingested_at BETWEEN (SELECT start_date FROM date_range) AND (SELECT end_date FROM date_range)
        )
        SELECT
          COALESCE(SUM(c.vl_pago), 0) AS unsent_vl_pago,
          COALESCE(SUM(c.vl_info), 0) AS unsent_vl_info
        FROM ingested_claims c
        LEFT JOIN `{full_submitted_claims_output_table}` s
          ON c.id_arvo = s.id_arvo
        WHERE s.id_arvo IS NULL
        """
        query_job = self.bq_client.query(unsent_claims_query)
        result = query_job.result()
        row = next(result, None)

        unsent_vl_pago = 0.0
        unsent_vl_info = 0.0
        if row:
            unsent_vl_pago = float(row.unsent_vl_pago or 0.0)
            unsent_vl_info = float(row.unsent_vl_info or 0.0)

        # Build labels: partner is required
        labels = { "partner": partner_value }

        # Emit metric representing the sum of vl_pago of unsent claims
        emit_gauge_metric(
            monitoring_client=self.monitoring_client,
            project_id=self.run_project_id,
            name="claims/pipeline/delta_recv_sent/vl_pago",
            value=unsent_vl_pago,
            labels=labels,
            timestamp=source_timestamp,
        )

        # Emit metric representing the sum of vl_info of unsent claims
        emit_gauge_metric(
            monitoring_client=self.monitoring_client,
            project_id=self.run_project_id,
            name="claims/pipeline/delta_recv_sent/vl_info",
            value=unsent_vl_info,
            labels=labels,
            timestamp=source_timestamp,
        )