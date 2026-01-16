# Import deployment commands
# mod local 'deployment/local/local.just'

default:
    @just --list

# === PostgreSQL (Docker) ===

# Start PostgreSQL in Docker
db-up:
    @just local up

# Stop PostgreSQL
db-down:
    @just local down

# View PostgreSQL logs
db-logs:
    @just local logs

# Connect to PostgreSQL with psql
db-connect:
    @just local db-connect

# Reset database (WARNING: deletes all data)
db-wipe:
    @just local wipe

# === Migrations (Local) ===

# Run migrations against Docker PostgreSQL
migrate:
    uv run alembic upgrade head

# Create new migration
migration name:
    uv run alembic revision -m "{{name}}"

# Show current migration version
migrate-status:
    uv run alembic current

# Show migration history
migrate-history:
    uv run alembic history

# Rollback one migration
migrate-down:
    uv run alembic downgrade -1

# === CLI (Local) ===

# Run any osa CLI command
cli *ARGS:
    uv run osa {{ARGS}}

# Initialize a field
init FIELD:
    uv run osa field init {{FIELD}}

# Wipe all local OSA data (database, vectors, cache)
wipe:
    uv run osa local clean --force

# === Complete Workflow ===

# Set up everything for first time (start DB + run migrations)
setup:
    @echo "Starting PostgreSQL..."
    @just db-up
    @echo "Waiting for PostgreSQL to be ready..."
    @sleep 3
    @echo "Running migrations..."
    @just migrate
    @echo "Setup complete! Database is ready."

# Aliases for common commands
up: db-up
down: db-down

# Testing commands
test kind="unit":
    @TEST=1 uv run pytest "tests/{{kind}}" -v --tb=short

test-s kind="unit":
    @TEST=1 uv run pytest -s -o log_cli=True -o log_cli_level=DEBUG "tests/{{kind}}"

test-unit:
    @TEST=1 uv run pytest tests/unit

[working-directory: 'deployment/local']
test-e2e:
    docker compose --profile test-e2e up --build --abort-on-container-exit test-e2e

[working-directory: 'deployment/local']
test-integration:
    docker compose --profile test up --build --abort-on-container-exit test

# Code quality commands
fix thing="osa":
    uv run ruff format {{thing}}
    uv run ruff check --fix {{thing}}

lint thing="osa":
    uv run ruff check {{thing}}
    uv run ty check {{thing}}

# Docker commands (standalone)
docker-build:
    docker build -t osa-api:latest .

docker-run PORT="8000":
    docker run -p {{PORT}}:8000 --env-file .env osa-api:latest

docker-serve PORT="8000":
    just docker-build && docker run -p {{PORT}}:8000 --env-file .env osa-api:latest

docker-shell docker-build:
    docker run -it --rm osa-api:latest bash

docker-stop:
    docker stop osa-api && docker rm osa-api || true

# Run database migrations locally
db-migrate:
    uv run alembic upgrade head

# Create new migration
db-migration name:
    uv run alembic revision -m "{{name}}"

# Show migration history
db-history:
    uv run alembic history

# Show current migration version
db-current:
    uv run alembic current

# Downgrade migration
db-downgrade:
    uv run alembic downgrade -1
