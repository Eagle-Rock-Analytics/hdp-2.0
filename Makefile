.PHONY: help install install-dev test lint format security pre-commit clean

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install project dependencies
	uv sync

install-dev: ## Install project + dev dependencies, set up pre-commit
	uv sync --extra dev
	uv run pre-commit install

test: ## Run tests
	uv run pytest

test-cov: ## Run tests with coverage
	uv run pytest --cov=scripts --cov-report=html --cov-report=term-missing

lint: ## Run all linters
	uv run ruff check scripts/ --fix

format: ## Format code
	uv run black scripts/ notebooks/
	uv run isort scripts/ notebooks/
	uv run ruff check --fix scripts/

security: ## Run security checks
	uv run bandit -r scripts/
	uv run detect-secrets scan --baseline .secrets.baseline

pre-commit: ## Run all pre-commit hooks against all files
	uv run pre-commit run --all-files

clean: ## Clean up temporary files and caches
	rm -rf .ruff_cache/
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -name "*.pyc" -delete
