# OSA Monorepo Justfile
# Production deployment and development orchestration commands

mod server

default:
    @just --list

# === Production Deployment ===

# Start all services in production mode
up:
    docker compose -f deploy/docker-compose.yml up -d

# Start all services with logs visible
up-attached:
    docker compose -f deploy/docker-compose.yml up

# Stop all services
down:
    docker compose -f deploy/docker-compose.yml down

# View logs for a service (e.g., just logs server, just logs db)
logs service:
    docker compose -f deploy/docker-compose.yml logs -f {{service}}

# Shell into the server container
server-shell:
    docker compose -f deploy/docker-compose.yml exec server bash

# Restart just the server
server-restart:
    docker compose -f deploy/docker-compose.yml restart server

# Restart all services
restart:
    docker compose -f deploy/docker-compose.yml restart

# Rebuild and restart services
rebuild:
    docker compose -f deploy/docker-compose.yml up -d --build

# === Development Mode ===

# Start full-stack development with hot-reload
dev:
    docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.dev.yml up

# Start development in background
dev-detached:
    docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.dev.yml up -d

# Stop development environment
dev-down:
    docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.dev.yml down

# Open the web UI in browser
open-ui:
    open http://localhost:8080

# === Code Quality ===

# Lint all code (server + web)
lint:
    cd server && just lint
    cd web && pnpm lint

# === Individual Service Development ===

# Run server independently (requires database)
server-dev:
    cd server && just dev

# Run web frontend independently
web-dev:
    cd web && pnpm dev

# Build web frontend for production
web-build:
    cd web && pnpm build

# Lint web frontend code
web-lint:
    cd web && pnpm lint

# === Database ===

# Start only the database
db-up:
    docker compose -f deploy/docker-compose.yml up -d db

# Stop the database
db-down:
    docker compose -f deploy/docker-compose.yml stop db

# Connect to PostgreSQL
db-connect:
    docker compose -f deploy/docker-compose.yml exec db psql -U postgres -d osa

# === Maintenance ===

# Clean up Docker resources (volumes, images, etc.)
clean:
    docker compose -f deploy/docker-compose.yml down -v --rmi local

# Show service status
status:
    docker compose -f deploy/docker-compose.yml ps
