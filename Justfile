# OSA Monorepo Justfile
# Production deployment and development orchestration commands

default:
    @just --list

# === Production Deployment ===

# Start all services in production mode
up:
    docker compose up -d

# Start all services with logs visible
up-attached:
    docker compose up

# Stop all services
down:
    docker compose down

# View logs from all services
logs:
    docker compose logs -f

# View logs from a specific service
logs-service service:
    docker compose logs -f {{service}}

# View server logs
server-logs:
    docker compose logs -f server

# View last N lines of server logs (default: 100)
server-logs-tail lines="100":
    docker compose logs --tail {{lines}} server

# View server logs with timestamps
server-logs-time:
    docker compose logs -f -t server

# View server logs since a time (e.g., "10m", "1h", "2024-01-01")
server-logs-since since:
    docker compose logs -f --since {{since}} server

# Shell into the server container
server-shell:
    docker compose exec server bash

# Restart just the server
server-restart:
    docker compose restart server

# Restart all services
restart:
    docker compose restart

# Rebuild and restart services
rebuild:
    docker compose up -d --build

# === Development Mode ===

# Start full-stack development with hot-reload
dev:
    docker compose -f docker-compose.yml -f docker-compose.dev.yml up

# Start development in background
dev-detached:
    docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d

# Stop development environment
dev-down:
    docker compose -f docker-compose.yml -f docker-compose.dev.yml down

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
    docker compose up -d db

# Stop the database
db-down:
    docker compose stop db

# View database logs
db-logs:
    docker compose logs -f db

# Connect to PostgreSQL
db-connect:
    docker compose exec db psql -U postgres -d osa

# === Maintenance ===

# Clean up Docker resources (volumes, images, etc.)
clean:
    docker compose down -v --rmi local

# Show service status
status:
    docker compose ps
