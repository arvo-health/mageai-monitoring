.PHONY: help install install-dev test lint lint-fix format typecheck clean local-up local-down local-seed local-run local-test docker-build docker-push

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	uv sync --no-dev

install-dev: ## Install development dependencies
	uv sync --all-extras

test: ## Run integration tests (requires Docker)
	uv run pytest tests/ -v -s

lint: ## Run linting checks
	uv run ruff check .

lint-fix: ## Auto-fix linting issues
	uv run ruff format .
	uv run ruff check --fix --unsafe-fixes .

format: ## Format code with ruff format and ruff check
	uv run ruff format .
	uv run ruff check --fix .

typecheck: ## Run type checking
	uv run mypy .

clean: ## Clean generated files and caches
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -r {} +
	find . -type d -name ".pytest_cache" -exec rm -r {} +
	find . -type d -name ".mypy_cache" -exec rm -r {} +
	find . -type d -name "htmlcov" -exec rm -r {} +
	rm -rf .ruff_cache

local-up: ## Start BigQuery emulator via docker-compose and seed with test data
	docker-compose up -d bigquery-emulator
	@echo "Waiting for BigQuery emulator to be ready..."
	@sleep 5
	@echo "Seeding BigQuery with test data..."
	@BIGQUERY_PROJECT_ID=test-project uv run python scripts/seed_bigquery/main.py || \
		(echo "Warning: Seeding failed. The emulator may still be starting. Try 'make local-seed' manually."; exit 0)
	@echo "BigQuery emulator started and seeded on http://localhost:9050"

local-seed: ## Seed BigQuery emulator with test data (requires emulator to be running)
	@echo "Seeding BigQuery with test data..."
	@BIGQUERY_PROJECT_ID=test-project uv run python scripts/seed_bigquery/main.py

local-down: ## Stop BigQuery emulator
	docker-compose down

local-run: ## Run the service in local mode (requires BigQuery emulator)
	@if ! docker-compose ps | grep -q "bigquery-emulator.*Up"; then \
		echo "Starting BigQuery emulator..."; \
		$(MAKE) local-up; \
	fi
	LOCAL_MODE=true uv run functions-framework --target=handle_cloud_event --port=8080

local-test: ## Test HTTP endpoint with example CloudEvent payload (requires service running)
	@echo "Testing local endpoint with example payload..."
	@curl -X POST http://localhost:8080 \
		-H "Content-Type: application/json" \
		-d @examples/pre_filtered_payload.json || \
		(echo "Error: Make sure the service is running (make local-run) and examples/pre_filtered_payload.json exists"; exit 1)

# Docker image configuration
IMAGE_REGISTRY := us-docker.pkg.dev/arvo-datalake/containers/mageai-monitoring
VERSION := $(shell awk -F'"' '/^version = / {print $$2}' pyproject.toml)

docker-build: ## Build Docker image with version tag
	@echo "Building Docker image: $(IMAGE_REGISTRY):$(VERSION)"
	docker build --platform linux/amd64 -t $(IMAGE_REGISTRY):$(VERSION) .
	@echo "Image built successfully: $(IMAGE_REGISTRY):$(VERSION)"

docker-push: docker-build ## Build and push Docker image to registry
	@echo "Pushing Docker image: $(IMAGE_REGISTRY):$(VERSION)"
	docker push $(IMAGE_REGISTRY):$(VERSION)
	@echo "Image pushed successfully: $(IMAGE_REGISTRY):$(VERSION)"

