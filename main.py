"""Cloud Run entry point for Mage.ai pipeline monitoring service."""

import logging

import functions_framework
from cloudevents.http import CloudEvent
from flask import make_response

from bigquery import create_bigquery_client
from config import Config
from dispatcher import HandlerDispatcher
from handlers.pipeline_run import PipelineRunHandler
from handlers.post_filtered_approval import PostFilteredApprovalHandler
from handlers.post_filtered_selection import PostFilteredSelectionHandler
from handlers.pre_filtered_approval import PreFilteredApprovalHandler
from handlers.pre_filtered_wrangling import PreFilteredWranglingHandler
from handlers.processable_approval import ProcessableApprovalHandler
from handlers.processable_wrangling import ProcessableWranglingHandler
from handlers.savings_approval import SavingsApprovalHandler
from metrics import create_monitoring_client

logging.basicConfig(level=logging.INFO)


def create_handlers(
    monitoring_client,
    bq_client,
    config: Config,
) -> list:
    """
    Create handler instances with injected dependencies.

    Args:
        monitoring_client: Monitoring client (GCP or logged)
        bq_client: BigQuery client
        config: Application configuration

    Returns:
        List of handler instances
    """
    return [
        PipelineRunHandler(
            monitoring_client=monitoring_client,
            run_project_id=config.cloud_run_project_id,
        ),
        PreFilteredApprovalHandler(
            monitoring_client=monitoring_client,
            bq_client=bq_client,
            run_project_id=config.cloud_run_project_id,
            data_project_id=config.bigquery_project_id,
        ),
        PreFilteredWranglingHandler(
            monitoring_client=monitoring_client,
            bq_client=bq_client,
            run_project_id=config.cloud_run_project_id,
            data_project_id=config.bigquery_project_id,
        ),
        PostFilteredSelectionHandler(
            monitoring_client=monitoring_client,
            bq_client=bq_client,
            run_project_id=config.cloud_run_project_id,
            data_project_id=config.bigquery_project_id,
        ),
        PostFilteredApprovalHandler(
            monitoring_client=monitoring_client,
            bq_client=bq_client,
            run_project_id=config.cloud_run_project_id,
            data_project_id=config.bigquery_project_id,
        ),
        ProcessableApprovalHandler(
            monitoring_client=monitoring_client,
            bq_client=bq_client,
            run_project_id=config.cloud_run_project_id,
            data_project_id=config.bigquery_project_id,
        ),
        ProcessableWranglingHandler(
            monitoring_client=monitoring_client,
            bq_client=bq_client,
            run_project_id=config.cloud_run_project_id,
            data_project_id=config.bigquery_project_id,
        ),
        SavingsApprovalHandler(
            monitoring_client=monitoring_client,
            bq_client=bq_client,
            run_project_id=config.cloud_run_project_id,
            data_project_id=config.bigquery_project_id,
        ),
    ]


# Lazy initialization - these are initialized on first request
_dispatcher = None


@functions_framework.cloud_event
def handle_cloud_event(cloud_event: CloudEvent):
    """
    Triggered by Pub/Sub message (production) or HTTP request (local).

    The same handler works for both modes:
    - Production: Receives CloudEvent from Pub/Sub subscription
    - Local: functions-framework automatically converts HTTP POST requests to CloudEvent format

    Routes to appropriate handlers based on event content.

    Uses lazy initialization to avoid macOS fork safety issues with Google Cloud clients.
    """
    global _dispatcher

    # Initialize on first request (happens after fork in each worker)
    if _dispatcher is None:
        config = Config()
        monitoring_client = create_monitoring_client(config)
        bq_client = create_bigquery_client(config)
        handlers = create_handlers(monitoring_client, bq_client, config)
        _dispatcher = HandlerDispatcher(
            handlers=handlers,
            monitoring_client=monitoring_client,
            project_id=config.cloud_run_project_id,
        )

    try:
        return _dispatcher.dispatch(cloud_event)

    except Exception as e:
        logging.error(f"Unhandled error: {e}", exc_info=True)
        # Returning 500 signals Pub/Sub to retry delivery (production)
        # or indicates server error (local)
        return make_response(("Internal Server Error", 500))
