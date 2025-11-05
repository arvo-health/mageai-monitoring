"""Handler for tracking and emitting pipeline execution metrics.

This handler processes all pipeline execution events from Mage.ai workflows
to emit count metrics that track pipeline run frequency and status. For each
pipeline execution event, it emits a metric that captures the pipeline
identifier, execution status, and partner information, enabling monitoring
and analysis of pipeline execution patterns across the system.
"""

from datetime import datetime
from typing import Dict
from google.cloud import monitoring_v3

from handlers.base import Handler, HandlerBadRequestError
from metrics import emit_gauge_metric


class PipelineRunHandler(Handler):
    
    def __init__(
        self,
        monitoring_client: monitoring_v3.MetricServiceClient,
        run_project_id: str,
    ):
        """
        Initialize the handler.
        
        Args:
            monitoring_client: GCP Monitoring client
            run_project_id: Project ID for metric emission
        """
        self.monitoring_client = monitoring_client
        self.run_project_id = run_project_id
    
    def match(self, decoded_message: Dict) -> bool:
        """
        Match all valid pipeline events.
        
        This handler processes all events that have the expected structure.
        Returns True if the event can be parsed, False otherwise.
        """
        if "payload" not in decoded_message or "source_timestamp" not in decoded_message:
            return False
        
        pl = decoded_message["payload"]
        if "pipeline_uuid" not in pl or "status" not in pl:
            return False
        
        return True
    
    def handle(self, decoded_message: Dict) -> None:
        """
        Emit the basic pipeline run metric.
        
        Args:
            decoded_message: The decoded message dictionary
            
        Raises:
            HandlerBadRequestError: If the required 'partner' variable is
                not present in the event payload
        """
        pl = decoded_message["payload"]
        variables = pl.get("variables", {})
        
        labels = {
            "pipeline_uuid": pl["pipeline_uuid"],
            "pipeline_status": pl["status"],
        }
        
        partner_value = variables.get("partner")
        if not partner_value:
            raise HandlerBadRequestError(
                "Missing required 'partner' variable in payload."
            )
        labels["partner"] = str(partner_value)
        
        source_timestamp = datetime.fromisoformat(
            decoded_message["source_timestamp"].replace("Z", "+00:00")
        )
        
        emit_gauge_metric(
            monitoring_client=self.monitoring_client,
            project_id=self.run_project_id,
            name="mageai/pipeline_run/count",
            value=1.0,
            labels=labels,
            timestamp=source_timestamp,
        )

