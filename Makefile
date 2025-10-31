.PHONY: help install install-dev test test-integration test-all lint format typecheck clean docker-up docker-down

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	uv sync --no-dev

install-dev: ## Install development dependencies
	uv sync --all-extras

test: ## Run unit tests
	uv run pytest tests/ -v -m "not integration"

test-integration: ## Run integration tests (requires Docker)
	uv run pytest tests_integration/ -v -m integration

test-all: ## Run all tests (unit + integration)
	uv run pytest tests/ tests_integration/ -v

lint: ## Run linting checks
	uv run ruff check src/ tests/ tests_integration/

format: ## Format code with black and ruff
	uv run black src/ tests/ tests_integration/
	uv run ruff check --fix src/ tests/ tests_integration/

typecheck: ## Run type checking
	uv run mypy src/

docker-up: ## Start BigQuery emulator container (optional, testcontainers manages containers automatically)
	docker-compose up -d bigquery-emulator

docker-down: ## Stop BigQuery emulator container
	docker-compose down

clean: ## Clean generated files and caches
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -r {} +
	find . -type d -name ".pytest_cache" -exec rm -r {} +
	find . -type d -name ".mypy_cache" -exec rm -r {} +
	find . -type d -name "htmlcov" -exec rm -r {} +
	rm -rf .ruff_cache

