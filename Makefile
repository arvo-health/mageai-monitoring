.PHONY: help install install-dev test lint format typecheck clean

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install production dependencies
	uv sync --no-dev

install-dev: ## Install development dependencies
	uv sync --all-extras

test: ## Run integration tests (requires Docker)
	uv run pytest tests/ -v -m integration -s

lint: ## Run linting checks
	uv run ruff check src/ tests/

format: ## Format code with black and ruff
	uv run black src/ tests/
	uv run ruff check --fix src/ tests/

typecheck: ## Run type checking
	uv run mypy src/

clean: ## Clean generated files and caches
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -r {} +
	find . -type d -name ".pytest_cache" -exec rm -r {} +
	find . -type d -name ".mypy_cache" -exec rm -r {} +
	find . -type d -name "htmlcov" -exec rm -r {} +
	rm -rf .ruff_cache

