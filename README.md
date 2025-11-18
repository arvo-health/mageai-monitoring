# Mage.ai Pipeline Run Monitoring

A Google Cloud Platform (GCP) service that monitors pipeline execution events from the Mage.ai workflow engine and publishes operational metrics to GCP Cloud Monitoring.

## Architecture

This service is deployed as a **Cloud Run service** that is triggered by **Cloud Pub/Sub** subscriptions. It processes change events from the `pipeline_run` table in a Cloud SQL database that tracks pipeline execution state in the Mage.ai workflow engine.

### Event Flow

1. **Change Detection**: Database change events are captured from the Cloud SQL `pipeline_run` table via Cloud SQL's change stream or logical replication mechanisms (configured separately).

2. **Message Publishing**: These change events are published to a Cloud Pub/Sub topic as messages.

3. **Event Processing**: This Cloud Run service consumes messages from the Pub/Sub subscription and processes each change event.

4. **Metric Emission**: For each processed event, one or more custom metrics are generated and published to **GCP Cloud Monitoring** using the Cloud Monitoring API.

## Technology Stack

- **Runtime**: Python
- **Package Management**: `uv`
- **Deployment**: GCP Cloud Run
- **Trigger**: Cloud Pub/Sub subscription
- **Monitoring**: GCP Cloud Monitoring
- **Data Source**: Cloud SQL (Mage.ai `pipeline_run` table)
- **Testing**: pytest with BigQuery emulator for integration tests

## Development

### Prerequisites

- Python 3.12+
- `uv` package manager ([installation guide](https://github.com/astral-sh/uv))
- Docker (for BigQuery emulator in integration tests)

### Setup

1. Install dependencies:
   ```bash
   make install
   ```

2. Run tests:
   ```bash
   make test
   ```

3. Run integration tests (requires Docker):
   ```bash
   make test-integration
   ```

### Project Structure

```
.
├── handlers/           # Handler modules for processing cloud events
├── dispatcher.py       # Handler dispatcher for routing events
├── metrics.py          # Utility functions for emitting metrics
├── main.py             # Cloud Run entry point
├── tests/               # Integration tests (with BigQuery emulator)
├── pyproject.toml       # Project configuration
├── Makefile            # Common commands
└── README.md           # This file
```

## Testing

Integration tests automatically start and manage a BigQuery emulator container using `testcontainers`. The emulator runs in Docker and provides a local BigQuery API endpoint for testing.

If you need to inspect containers for debugging, you can keep them by setting an environment variable:
```bash
make test
```

See `Makefile` for all available commands (`make help`).

## Local Development

The service can be run locally for development and testing. In local mode, the service:
- Accepts HTTP POST requests (same CloudEvent format as production)
- Uses BigQuery emulator instead of real GCP BigQuery
- Logs metrics as structured JSON instead of sending to GCP Cloud Monitoring
- Uses default project IDs suitable for local development

### Prerequisites

- Docker (for BigQuery emulator)
- All development dependencies installed (`make install-dev`)

### Running Locally

1. **Start the BigQuery emulator and seed test data:**
   ```bash
   make local-up
   ```
   This starts a BigQuery emulator container on `localhost:9050` and automatically seeds it with test datasets and tables that match the example payload. The seeding creates:
   - Dataset: `tmp_abertta` in the `test-project` project
   - Tables with sample claim data:
     - `unprocessable_claims_abertta_20251105_171427` (total `vl_pago`: 1500.0)
     - `processable_claims_abertta_20251105_171427` (total `vl_pago`: 8000.0)

2. **Run the service in local mode:**
   ```bash
   make local-run
   ```
   This starts the service on `http://localhost:8080` with local mode enabled. The service will automatically start the BigQuery emulator if it's not already running.

3. **Test the service:**
   ```bash
   make local-test
   ```
   This sends the example payload to the running service and validates the response.

4. **Stop the emulator when done:**
   ```bash
   make local-down
   ```

### Configuration

Local mode is enabled by setting the `LOCAL_MODE` environment variable (automatically set by `make local-run`). The following defaults are used in local mode (can be overridden with environment variables):
- `LOCAL_MODE`: `true` (enables local mode - set automatically by `make local-run`)
- `RUN_PROJECT_ID`: `local-project` (default in local mode)
- `DATA_PROJECT_ID`: `test-project` (default in local mode, matches emulator)
- `BIGQUERY_EMULATOR_HOST`: `localhost:9050` (default)

**Re-seeding BigQuery:**
If you need to reset or re-seed the BigQuery emulator with test data, you can run:
```bash
make local-seed
```
This is automatically done when running `make local-up`, but can be useful if you've modified the emulator state and want to restore the initial test data.

You can override these by setting environment variables before running:
```bash
export RUN_PROJECT_ID=my-local-project
export DATA_PROJECT_ID=test-project
export LOCAL_MODE=true
uv run functions-framework --target=handle_cloud_event --port=8080
```

Or manually run with custom settings:
```bash
LOCAL_MODE=true RUN_PROJECT_ID=my-project uv run functions-framework --target=handle_cloud_event --port=8080
```

### Making HTTP Requests

The service accepts HTTP POST requests with CloudEvent JSON payloads. The same handler works for both local development and production deployment.

**Example CloudEvent payload** (save as `test_payload.json`):

```json
{
  "type": "google.cloud.pubsub.topic.v1.messagePublished",
  "source": "//pubsub.googleapis.com/projects/local-project/topics/local-topic",
  "specversion": "1.0",
  "id": "local-test-event-123",
  "data": {
    "uuid": "f23df35c-01c0-4e72-bf69-d6c300000000",
    "read_timestamp": "2025-11-05T18:23:46.698000Z",
    "source_timestamp": "2025-11-05T18:23:31.393000Z",
    "object": "public_pipeline_run",
    "read_method": "postgresql-cdc",
    "stream_name": "projects/575686610682/locations/us-east1/streams/mageai-cdc-stream",
    "schema_key": "69db90b9a8c1a263696eb5cbdf57404bd2a9b7f4",
    "sort_keys": [1762367011393, "BD/A0007310"],
    "source_metadata": {
      "schema": "public",
      "table": "pipeline_run",
      "is_deleted": false,
      "change_type": "UPDATE",
      "tx_id": 1186572,
      "lsn": "BD/A0007310",
      "primary_keys": ["id"]
    },
    "payload": {
      "id": 15204,
      "created_at": "2025-11-05T18:19:39.044331Z",
      "updated_at": "2025-11-05T18:23:30.205321Z",
      "pipeline_schedule_id": 224,
      "pipeline_uuid": "pipesv2_wrangling",
      "execution_date": "2025-11-05T18:19:39.277871Z",
      "status": "COMPLETED",
      "completed_at": "2025-11-05T18:23:31.390594Z",
      "variables": {
        "pre_processing_filters": null,
        "refined_unprocessable_claims_output_table": "tmp_abertta.unprocessable_claims_abertta_20251105_171427",
        "refined_processable_claims_output_table": "tmp_abertta.processable_claims_abertta_20251105_171427",
        "data_quality": {
          "version": "781bd4888976390eff17bfb30179050f97b845a4",
          "function_path": "workflows_v2.data_quality.skip_function",
          "kwargs": {}
        },
        "partner": "abertta",
        "execution_date": "2025-11-05T17:14:27.412737+00:00",
        "gcs_input_csv": {
          "bucket_name": "pipes_v2",
          "path": "claims_current_abertta_20251105_171427.csv"
        },
        "run_id": "abertta_20251105_171427",
        "gcs_lock_file": {
          "bucket_name": "pipes_v2",
          "path": "prepare_raw_claims/abertta.lock",
          "disabled": false
        },
        "anonymization": {
          "version": "781bd4888976390eff17bfb30179050f97b845a4",
          "function_path": "workflows_v2.anonymization.skip_function",
          "kwargs": {}
        },
        "data_wrangling": {
          "version": "781bd4888976390eff17bfb30179050f97b845a4",
          "function_path": "workflows_v2.data_wrangling.arvo_api.default_wrangle",
          "kwargs": {
            "partner": "abertta"
          }
        },
        "data_wrangling_validation": {
          "version": "781bd4888976390eff17bfb30179050f97b845a4",
          "function_path": "workflows_v2.data_wrangling_validation.dummy_function",
          "kwargs": {}
        },
        "execution_partition": "224/20251105T181939_277871"
      },
      "passed_sla": false,
      "event_variables": null,
      "metrics": null,
      "backfill_id": null,
      "executor_type": "LOCAL_PYTHON",
      "started_at": "2025-11-05T18:19:40.121835Z"
    }
  }
}
```

**Send a test request:**

```bash
curl -X POST http://localhost:8080 \
  -H "Content-Type: application/json" \
  -d @test_payload.json
```

Or use the Makefile command:
```bash
make local-test
```

**Expected Response:**
- HTTP status: `204 No Content` (success) or `200 OK`
- The service logs will show structured JSON metric output like:
  ```
  INFO:root:METRIC: {"metric_type": "gauge", "project_id": "local-project", "name": "claims/pipeline/filtered_pre/vl_pago/total", "value": 1500.0, "labels": {"partner": "abertta", "approved": "false"}, "timestamp": "2025-11-05T18:23:31.393000+00:00"}
  INFO:root:METRIC: {"metric_type": "gauge", "project_id": "local-project", "name": "claims/pipeline/filtered_pre/vl_pago/relative", "value": 0.1875, "labels": {"partner": "abertta", "approved": "false"}, "timestamp": "2025-11-05T18:23:31.393000+00:00"}
  ```

The metrics are calculated by querying the seeded BigQuery tables:
- **Total value**: Sum of `vl_pago` from unprocessable claims table (1500.0)
- **Relative value**: Ratio of unprocessable to processable claims (1500.0 / 8000.0 = 0.1875)

### What's Different in Local Mode

- **BigQuery**: Uses local emulator instead of GCP BigQuery
- **Metrics**: Logged as structured JSON instead of sent to GCP Cloud Monitoring
- **Request Format**: Same CloudEvent format as production (functions-framework handles conversion)
- **Project IDs**: Default to local-friendly values (`local-project`, `test-project`)

### Production Deployment

In production, the service:
- Receives CloudEvents from Pub/Sub subscriptions (not HTTP)
- Uses real GCP BigQuery (requires authentication)
- Sends metrics to GCP Cloud Monitoring
- Uses production project IDs (`arvo-eng-prd`, `arvo-data-platform-prd`)

The same codebase works for both modes - no code changes needed for deployment.