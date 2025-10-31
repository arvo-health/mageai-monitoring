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

- Python 3.11+
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
├── src/                 # Source code
├── tests/               # Unit tests
├── tests_integration/   # Integration tests (with BigQuery emulator)
├── pyproject.toml       # Project configuration
├── Makefile            # Common commands
├── docker-compose.yml  # BigQuery emulator service (optional, for local dev)
└── README.md           # This file
```

## Testing

Integration tests automatically start and manage a BigQuery emulator container using `testcontainers`. The emulator runs in Docker and provides a local BigQuery API endpoint for testing.

If you need to inspect containers for debugging, you can keep them by setting an environment variable:
```bash
make test-integration
```

See `Makefile` for all available commands (`make help`).