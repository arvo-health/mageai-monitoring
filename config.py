"""Application configuration."""

import logging
import os
import sys

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    """
    Application configuration.
    
    Loads configuration from environment variables and command line flags.
    
    Environment variables (checked in order: prefixed, then plain):
    - {PREFIX}_LOCAL_MODE or LOCAL_MODE (default: true if not set)
    - {PREFIX}_CLOUD_RUN_PROJECT_ID or CLOUD_RUN_PROJECT_ID (default: "local-project" if not set)
    - {PREFIX}_BIGQUERY_PROJECT_ID or BIGQUERY_PROJECT_ID (default: "test-project" if not set)
    
    Command line flags (checked via sys.argv):
    - --local: Sets local mode
    
    Args:
        env_prefix: Prefix for environment variables (default: "MAGEAI_MONITORING")
    """
    
    cloud_run_project_id: str = Field(default="local-project")
    bigquery_project_id: str = Field(default="test-project")
    local_mode: bool = Field(default=True)
    
    model_config = SettingsConfigDict(
        env_prefix="MAGEAI_MONITORING_",
        env_file=None,  # Don't read .env files
        case_sensitive=False,
        extra="ignore",
    )
    
    def __init__(self, env_prefix: str = "MAGEAI_MONITORING", **kwargs):
        # Handle --local command line flag (highest priority)
        if "--local" in sys.argv:
            kwargs["local_mode"] = True
        
        # Helper to get env var with prefix fallback
        def get_env_with_fallback(key: str, default: str) -> str:
            prefixed_key = f"{env_prefix}_{key}"
            return os.getenv(prefixed_key, os.getenv(key, default))
        
        # Check for non-prefixed env vars as fallback (pydantic handles prefixed automatically)
        # Only override if not already set in kwargs
        if "cloud_run_project_id" not in kwargs:
            cloud_run_id = get_env_with_fallback("CLOUD_RUN_PROJECT_ID", "local-project")
            kwargs["cloud_run_project_id"] = cloud_run_id
        
        if "bigquery_project_id" not in kwargs:
            bq_id = get_env_with_fallback("BIGQUERY_PROJECT_ID", "test-project")
            kwargs["bigquery_project_id"] = bq_id
        
        if "local_mode" not in kwargs:
            prefixed_local_mode = os.getenv(f"{env_prefix}_LOCAL_MODE", "")
            plain_local_mode = os.getenv("LOCAL_MODE", "")
            local_mode_env = prefixed_local_mode or plain_local_mode
            
            if local_mode_env.lower() == "true":
                kwargs["local_mode"] = True
            elif local_mode_env.lower() == "false":
                kwargs["local_mode"] = False
            # else keep default (True)
        
        super().__init__(**kwargs)
        
        # Log configuration
        mode_name = "LOCAL" if self.local_mode else "PRODUCTION"
        logging.info(f"Running in {mode_name} mode")
        logging.info(f"CLOUD_RUN_PROJECT_ID: {self.cloud_run_project_id}")
        logging.info(f"BIGQUERY_PROJECT_ID: {self.bigquery_project_id}")


__all__ = ["Config"]
