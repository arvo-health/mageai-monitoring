"""Base handler for new beneficiaries handlers.

This module contains the base class for handlers that calculate and emit
new beneficiaries metrics from pipeline completion events.
"""

from datetime import datetime, timedelta

from google.api_core.exceptions import NotFound
from google.cloud import bigquery, monitoring_v3

from handlers.base import Handler, HandlerBadRequestError
from metrics import emit_gauge_metric


class NewBeneficiariesBaseHandler(Handler):
    """
    Base handler for calculating and emitting new beneficiaries metrics.

    This base class provides common functionality for handlers that process
    pipeline completion events to compute metrics about new beneficiaries (beneficiaries
    not present in a 3-month rolling history). Subclasses should implement
    the `match` method and call `_handle_new_beneficiaries_metrics` with
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

    def _handle_new_beneficiaries_metrics(
        self,
        decoded_message: dict,
        batch_processable_table_var: str,
        batch_unprocessable_table_var: str,
        historical_processable_table_var: str,
        historical_unprocessable_table_var: str,
        approved_value: str,
    ) -> None:
        """
        Calculate new beneficiaries metrics and emit to Cloud Monitoring.

        Queries BigQuery to identify beneficiaries in the batch that are not present
        in the historical data (3-month rolling window), then emits metrics
        representing the percentage of new beneficiaries per category.

        Args:
            decoded_message: The decoded message dictionary containing pipeline completion data
            pipeline_uuid: The pipeline UUID to verify (for defensive check)
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

        # Query to calculate new beneficiaries percentage per category
        # This query is compatible with both BigQuery and BigQuery emulator (SQLite)
        # Excludes batch items from historical lookup to avoid counting them as existing
        new_beneficiaries_query = f"""
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
        batch_beneficiaries AS (
            SELECT DISTINCT
                id_matricula,
                categoria
            FROM (
                SELECT id_matricula, categoria
                FROM `{full_batch_processable_table}`
                WHERE id_matricula IS NOT NULL AND categoria IS NOT NULL
                UNION ALL
                SELECT id_matricula, categoria
                FROM `{full_batch_unprocessable_table}`
                WHERE id_matricula IS NOT NULL AND categoria IS NOT NULL
            )
        ),
        historical_beneficiaries AS (
            SELECT DISTINCT
                id_matricula,
                categoria
            FROM (
                SELECT
                    id_matricula,
                    categoria
                FROM `{full_historical_processable_table}` h1
                WHERE created_at >= '{three_months_ago_date}'
                    AND id_matricula IS NOT NULL
                    AND categoria IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1
                        FROM batch_id_arvo b
                        WHERE b.id_arvo = h1.id_arvo
                    )
                UNION ALL
                SELECT
                    id_matricula,
                    categoria
                FROM `{full_historical_unprocessable_table}` h1
                WHERE created_at >= '{three_months_ago_date}'
                    AND id_matricula IS NOT NULL
                    AND categoria IS NOT NULL
                    AND NOT EXISTS (
                        SELECT 1
                        FROM batch_id_arvo b
                        WHERE b.id_arvo = h1.id_arvo
                    )
            )
        ),
        batch_counts AS (
            SELECT
                categoria,
                COUNT(DISTINCT id_matricula) AS total_beneficiaries
            FROM batch_beneficiaries
            GROUP BY categoria
        ),
        new_beneficiaries_counts AS (
            SELECT
                bb.categoria,
                COUNT(DISTINCT bb.id_matricula) AS new_beneficiaries
            FROM batch_beneficiaries bb
            LEFT JOIN historical_beneficiaries hb
                ON bb.id_matricula = hb.id_matricula
                AND bb.categoria = hb.categoria
            WHERE hb.id_matricula IS NULL
            GROUP BY bb.categoria
        )
        SELECT
            bc.categoria,
            bc.total_beneficiaries,
            COALESCE(nbc.new_beneficiaries, 0) AS new_beneficiaries,
            CASE
                WHEN bc.total_beneficiaries > 0 THEN
                    CAST(COALESCE(nbc.new_beneficiaries, 0) AS FLOAT64) / bc.total_beneficiaries
                ELSE 0.0
            END AS new_pct
        FROM batch_counts bc
        LEFT JOIN new_beneficiaries_counts nbc
            ON bc.categoria = nbc.categoria
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
                query_job = self.bq_client.query(new_beneficiaries_query)
                result = query_job.result()
                for row in result:
                    categoria = str(row.categoria)
                    new_pct = float(row.new_pct or 0.0)

                    # Build labels with category
                    labels = {**base_labels, "category": categoria}

                    # Emit metric
                    emit_gauge_metric(
                        monitoring_client=self.monitoring_client,
                        project_id=self.run_project_id,
                        name="claims/beneficiaries/new_pct_3mo",
                        value=new_pct,
                        labels=labels,
                        timestamp=source_timestamp,
                    )
            else:
                # If historical tables don't exist, assume all beneficiaries are new (100%)
                # Query only batch tables to get categories
                batch_categories_query = f"""
                SELECT DISTINCT categoria
                FROM (
                    SELECT categoria
                    FROM `{full_batch_processable_table}`
                    WHERE categoria IS NOT NULL
                    UNION ALL
                    SELECT categoria
                    FROM `{full_batch_unprocessable_table}`
                    WHERE categoria IS NOT NULL
                )
                """
                query_job = self.bq_client.query(batch_categories_query)
                result = query_job.result()
                for row in result:
                    categoria = str(row.categoria)

                    # Build labels with category
                    labels = {**base_labels, "category": categoria}

                    # Emit metric with 100% new beneficiaries
                    emit_gauge_metric(
                        monitoring_client=self.monitoring_client,
                        project_id=self.run_project_id,
                        name="claims/beneficiaries/new_pct_3mo",
                        value=1.0,
                        labels=labels,
                        timestamp=source_timestamp,
                    )

        except NotFound:
            # If batch tables don't exist, raise error
            raise HandlerBadRequestError(
                f"Batch tables not found: {batch_processable_table} or {batch_unprocessable_table}"
            )
