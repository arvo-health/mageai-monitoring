"""Base handler for providers volume ratio handlers.

This module contains the base class for handlers that calculate and emit
providers volume ratio metrics from pipeline completion events.
"""

from datetime import datetime, timedelta

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, monitoring_v3

from handlers.base import Handler, HandlerBadRequestError
from metrics import emit_gauge_metric


class ProvidersVolumeRatioBaseHandler(Handler):
    """
    Base handler for calculating and emitting providers volume ratio metrics.

    This base class provides common functionality for handlers that process
    pipeline completion events to compute metrics about providers volume ratio.
    The ratio is the number of providers in the last 30 days, including the latest batch,
    divided by the number of providers in the previous 30 days period (D-60 to D-30).
    Subclasses should implement the `match` method and call
    `_handle_providers_volume_ratio_metrics` with pipeline-specific configuration.
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

    def _handle_providers_volume_ratio_metrics(
        self,
        decoded_message: dict,
        batch_processable_table_var: str,
        batch_unprocessable_table_var: str,
        historical_processable_table_var: str,
        historical_unprocessable_table_var: str,
        approved_value: str,
    ) -> None:
        """
        Calculate providers volume ratio metrics and emit to Cloud Monitoring.

        Queries BigQuery to calculate the ratio of the number of providers in the last 30 days,
        including the latest batch, to the number of providers in the previous 30 days period
        (D-60 to D-30).

        Args:
            decoded_message: The decoded message dictionary containing pipeline completion data
            batch_processable_table_var: Variable name for the batch processable claims table
            batch_unprocessable_table_var: Variable name for the batch unprocessable claims table
            historical_processable_table_var: Variable name for the historical processable
                claims table
            historical_unprocessable_table_var: Variable name for the historical
                unprocessable claims table
            approved_value: Value for the "approved" label ("true" or "false")

        Raises:
            HandlerBadRequestError: If the required input table variables are not present
                in the event, or if the payload is missing
        """
        payload = decoded_message.get("payload")

        if not payload:
            raise HandlerBadRequestError("No 'payload' found in event data.")

        variables = payload.get("variables", {})

        batch_processable_table = variables.get(batch_processable_table_var)
        batch_unprocessable_table = variables.get(batch_unprocessable_table_var)
        historical_processable_table = variables.get(historical_processable_table_var)
        historical_unprocessable_table = variables.get(historical_unprocessable_table_var)

        if not batch_processable_table:
            raise HandlerBadRequestError(
                f"No variable '{batch_processable_table_var}' found in payload."
            )

        if not batch_unprocessable_table:
            raise HandlerBadRequestError(
                f"No variable '{batch_unprocessable_table_var}' found in payload."
            )

        if not historical_processable_table:
            raise HandlerBadRequestError(
                f"No variable '{historical_processable_table_var}' found in payload."
            )

        if not historical_unprocessable_table:
            raise HandlerBadRequestError(
                f"No variable '{historical_unprocessable_table_var}' found in payload."
            )

        # Ensure fully-qualified table paths
        def ensure_full_table_ref(table: str) -> str:
            return table if "." in table else f"{self.data_project_id}.{table}"

        full_batch_processable_table = ensure_full_table_ref(batch_processable_table)
        full_batch_unprocessable_table = ensure_full_table_ref(batch_unprocessable_table)
        full_historical_processable_table = ensure_full_table_ref(historical_processable_table)
        full_historical_unprocessable_table = ensure_full_table_ref(historical_unprocessable_table)

        # Parse timestamp for 3-month window calculation
        source_timestamp = datetime.fromisoformat(
            decoded_message["source_timestamp"].replace("Z", "+00:00")
        )
        one_month_ago = source_timestamp - timedelta(days=30)
        two_months_ago = source_timestamp - timedelta(days=60)
        # Format as date only (YYYY-MM-DD) for simple comparison
        one_month_ago_date = one_month_ago.strftime("%Y-%m-%d")
        two_months_ago_date = two_months_ago.strftime("%Y-%m-%d")

        # Build base labels
        partner_value = variables.get("partner")
        if not partner_value:
            raise HandlerBadRequestError("Missing required 'partner' variable in payload.")
        base_labels = {
            "partner": str(partner_value),
            "approved": approved_value,
        }

        # Query to calculate providers volume ratio
        providers_volume_ratio_query = f"""
        WITH latest_providers AS (
            SELECT id_prestador
            FROM `{full_batch_processable_table}`
            WHERE id_prestador IS NOT NULL
            UNION DISTINCT
            SELECT id_prestador
            FROM `{full_batch_unprocessable_table}`
            WHERE id_prestador IS NOT NULL
            UNION DISTINCT
            SELECT id_prestador
            FROM `{full_historical_processable_table}` h1
            WHERE created_at >= '{one_month_ago_date}'
                AND id_prestador IS NOT NULL
            UNION DISTINCT
            SELECT id_prestador
            FROM `{full_historical_unprocessable_table}` h1
            WHERE created_at >= '{one_month_ago_date}'
                AND id_prestador IS NOT NULL
        ),
        previous_providers AS (
            SELECT id_prestador
            FROM `{full_historical_processable_table}` h1
            WHERE created_at >= '{two_months_ago_date}'
                AND created_at < '{one_month_ago_date}'
                AND id_prestador IS NOT NULL
            UNION DISTINCT
            SELECT id_prestador
            FROM `{full_historical_unprocessable_table}` h1
            WHERE created_at >= '{two_months_ago_date}'
                AND created_at < '{one_month_ago_date}'
                AND id_prestador IS NOT NULL
        ),
        provider_counts AS (
            SELECT
                COUNT(DISTINCT l.id_prestador) AS latest_count,
                COUNT(DISTINCT p.id_prestador) AS previous_count
            FROM latest_providers l CROSS JOIN previous_providers p
        )
        SELECT
            latest_count,
            previous_count,
            CASE
                WHEN previous_count > 0 AND latest_count > 0 THEN
                    SAFE_DIVIDE(latest_count, previous_count)
                WHEN previous_count = 0 THEN
                    1.0
                ELSE
                    0.0
            END AS ratio
        FROM provider_counts
        """

        try:
            # Check if batch tables exist
            self.bq_client.get_table(full_batch_processable_table)
            self.bq_client.get_table(full_batch_unprocessable_table)

            # Check if historical tables exist
            historical_tables_exist = True
            try:
                self.bq_client.get_table(full_historical_processable_table)
                self.bq_client.get_table(full_historical_unprocessable_table)
            except NotFound:
                historical_tables_exist = False

            if historical_tables_exist:
                # Query with historical tables
                query_job = self.bq_client.query(providers_volume_ratio_query)
                result = query_job.result()
                for row in result:
                    ratio = float(row.ratio)

                    # Emit metric
                    emit_gauge_metric(
                        monitoring_client=self.monitoring_client,
                        project_id=self.run_project_id,
                        name="claims/pipeline/providers/volume_ratio_last_1_mo",
                        value=ratio,
                        labels=base_labels,
                        timestamp=source_timestamp,
                    )
            else:
                # If historical tables don't exist, assume the volume ratio is 1.0
                emit_gauge_metric(
                    monitoring_client=self.monitoring_client,
                    project_id=self.run_project_id,
                    name="claims/pipeline/providers/volume_ratio_last_1_mo",
                    value=1.0,
                    labels=base_labels,
                    timestamp=source_timestamp,
                )

        except NotFound:
            # If batch tables don't exist, raise error
            raise HandlerBadRequestError(
                f"Batch tables not found: {batch_processable_table} or {batch_unprocessable_table}"
            )
