import logging

import functions_framework
from flask import make_response
from cloudevents.http import CloudEvent
from google.cloud import monitoring_v3
from google.cloud import bigquery

from src.dispatcher import HandlerDispatcher
from src.handlers.pipeline_run import PipelineRunHandler
from src.handlers.pre_filtered_approval import PreFilteredApprovalHandler

# --- Configuration ---
RUN_PROJECT_ID = "arvo-eng-prd"  # Cloud Run project (for metrics)
DATA_PROJECT_ID = "arvo-data-platform-prd"  # BigQuery data project

logging.basicConfig(level=logging.INFO)


@functions_framework.cloud_event
def log_and_metric_pubsub(cloud_event: CloudEvent):
    """Triggered by Pub/Sub message. Routes to appropriate handlers."""
    try:
        monitoring_client = monitoring_v3.MetricServiceClient()
        bq_client = bigquery.Client(project=DATA_PROJECT_ID)

        handlers = [
            PipelineRunHandler(
                monitoring_client=monitoring_client,
                run_project_id=RUN_PROJECT_ID,
            ),
            PreFilteredApprovalHandler(
                monitoring_client=monitoring_client,
                bq_client=bq_client,
                run_project_id=RUN_PROJECT_ID,
                data_project_id=DATA_PROJECT_ID,
            ),
        ]

        dispatcher = HandlerDispatcher(handlers)
        return dispatcher.dispatch(cloud_event)

    except Exception as e:
        logging.error(f"Unhandled error: {e}", exc_info=True)
        # Returning 500 signals Pub/Sub to retry delivery
        return make_response(("Internal Server Error", 500))
