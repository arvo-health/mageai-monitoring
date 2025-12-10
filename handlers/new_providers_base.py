"""Base handler for new providers handlers.

This module contains the base class for handlers that calculate and emit
new providers metrics from pipeline completion events.
"""

from datetime import datetime, timedelta

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, monitoring_v3

from handlers.base import Handler, HandlerBadRequestError
from metrics import emit_gauge_metric


class NewProvidersBaseHandler(Handler):
    """
    Base handler for calculating and emitting new providers metrics.

    This base class provides common functionality for handlers that process
    pipeline completion events to compute metrics about new providers (providers
    not present in a 3-month rolling history). Subclasses should implement
    the `match` method and call `_handle_new_providers_metrics` with
    pipeline-specific configuration.
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

    def _handle_new_providers_metrics(
        self,
        decoded_message: dict,
        batch_processable_table_var: str,
        batch_unprocessable_table_var: str,
        historical_processable_table_var: str,
        historical_unprocessable_table_var: str,
        approved_value: str,
    ) -> None:
        """
        Calculate new providers metrics and emit to Cloud Monitoring.

        Queries BigQuery to identify providers in the batch that are not present
        in the historical data (3-month rolling window), then emits metrics
        representing the percentage of new providers.

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
        three_months_ago = source_timestamp - timedelta(days=90)
        # Format as date only (YYYY-MM-DD) for simple comparison
        # This works directly with DATETIME and TIMESTAMP columns in BigQuery and emulator
        three_months_ago_date = three_months_ago.strftime("%Y-%m-%d")

        # Build base labels
        partner_value = variables.get("partner")
        if not partner_value:
            raise HandlerBadRequestError("Missing required 'partner' variable in payload.")
        base_labels = {
            "partner": str(partner_value),
            "approved": approved_value,
        }

        # Query to calculate new providers percentage
        # This query is compatible with both BigQuery and BigQuery emulator (SQLite)
        # Excludes batch items from historical lookup to avoid counting them as existing
        new_providers_query = f"""
        WITH batch_id_arvo AS (
            SELECT DISTINCT id_arvo
            FROM (
                SELECT id_arvo
                FROM `{full_batch_processable_table}`
                WHERE id_arvo IS NOT NULL
                UNION ALL
                SELECT id_arvo
                FROM `{full_batch_unprocessable_table}`
                WHERE id_arvo IS NOT NULL
            )
        ),
        batch_providers AS (
            SELECT DISTINCT id_prestador
            FROM (
                SELECT id_prestador
                FROM `{full_batch_processable_table}`
                WHERE id_prestador IS NOT NULL
                UNION ALL
                SELECT id_prestador
                FROM `{full_batch_unprocessable_table}`
                WHERE id_prestador IS NOT NULL
            )
        ),
        historical_providers AS (
            SELECT DISTINCT id_prestador
            FROM (
                SELECT id_prestador
                FROM `{full_historical_processable_table}` h1
                WHERE created_at >= '{three_months_ago_date}'
                    AND id_prestador IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1
                        FROM batch_id_arvo b
                        WHERE b.id_arvo = h1.id_arvo
                    )
                UNION ALL
                SELECT id_prestador
                FROM `{full_historical_unprocessable_table}` h1
                WHERE created_at >= '{three_months_ago_date}'
                    AND id_prestador IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1
                        FROM batch_id_arvo b
                        WHERE b.id_arvo = h1.id_arvo
                    )
            )
        ),
        provider_counts AS (
            SELECT
                COUNT(DISTINCT bp.id_prestador) AS total_providers,
                COUNT(
                    DISTINCT CASE WHEN hp.id_prestador IS NULL THEN bp.id_prestador END
                ) AS new_providers
            FROM batch_providers bp
            LEFT JOIN historical_providers hp
                ON bp.id_prestador = hp.id_prestador
        )
        SELECT
            total_providers,
            COALESCE(new_providers, 0) AS new_providers,
            CASE
                WHEN total_providers > 0 THEN
                    CAST(COALESCE(new_providers, 0) AS FLOAT64) / total_providers
                ELSE 0.0
            END AS new_pct
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
                query_job = self.bq_client.query(new_providers_query)
                result = query_job.result()
                for row in result:
                    new_pct = float(row.new_pct or 0.0)

                    # Emit metric
                    emit_gauge_metric(
                        monitoring_client=self.monitoring_client,
                        project_id=self.run_project_id,
                        name="claims/providers/new_pct_3mo",
                        value=new_pct,
                        labels=base_labels,
                        timestamp=source_timestamp,
                    )
            else:
                # If historical tables don't exist, assume all providers are new (100%)
                # Emit metric with 100% new providers
                emit_gauge_metric(
                    monitoring_client=self.monitoring_client,
                    project_id=self.run_project_id,
                    name="claims/providers/new_pct_3mo",
                    value=1.0,
                    labels=base_labels,
                    timestamp=source_timestamp,
                )

        except NotFound:
            # If batch tables don't exist, raise error
            raise HandlerBadRequestError(
                f"Batch tables not found: {batch_processable_table} or {batch_unprocessable_table}"
            )
