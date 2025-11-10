"""Application configuration."""

import logging
from typing import Any

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    """
    Application configuration.

    Loads configuration from environment variables.

    Environment variables (checked in order: prefixed, then plain):
    - MAGEAI_MONITORING_LOCAL_MODE or LOCAL_MODE (default: true if not set)
    - MAGEAI_MONITORING_CLOUD_RUN_PROJECT_ID or CLOUD_RUN_PROJECT_ID (default: "local-project")
    - MAGEAI_MONITORING_BIGQUERY_PROJECT_ID or BIGQUERY_PROJECT_ID (default: "test-project")
    """

    cloud_run_project_id: str = Field(
        default="local-project",
        validation_alias=AliasChoices(
            "MAGEAI_MONITORING_CLOUD_RUN_PROJECT_ID", "CLOUD_RUN_PROJECT_ID"
        ),
    )
    bigquery_project_id: str = Field(
        default="test-project",
        validation_alias=AliasChoices(
            "MAGEAI_MONITORING_BIGQUERY_PROJECT_ID", "BIGQUERY_PROJECT_ID"
        ),
    )
    local_mode: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            "MAGEAI_MONITORING_LOCAL_MODE",
            "LOCAL_MODE",
        ),
    )

    model_config = SettingsConfigDict(
        env_file=None,  # Don't read .env files
        case_sensitive=False,
        extra="ignore",
        # Disable CLI parsing to avoid conflicts with functions-framework args
        cli_parse_args=False,
    )

    def __init__(self, **kwargs: Any):
        """Initialize config and log configuration."""
        super().__init__(**kwargs)

        # Log configuration
        mode_name = "LOCAL" if self.local_mode else "PRODUCTION"
        logging.info(f"Running in {mode_name} mode")
        logging.info(f"CLOUD_RUN_PROJECT_ID: {self.cloud_run_project_id}")
        logging.info(f"BIGQUERY_PROJECT_ID: {self.bigquery_project_id}")


__all__ = ["Config"]
