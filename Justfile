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

# === Seed ===

# Seed the database with sample data (run while dev is up)
seed:
    docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.dev.yml exec server /app/.venv/bin/python /app/scripts/seed.py

# === Database ===

# Start only the database (dev mode — exposes port to host)
db-up:
    docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.dev.yml up -d db --wait

# Stop the database
db-down:
    docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.dev.yml stop db

# Connect to PostgreSQL
db-connect:
    docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.dev.yml exec db psql -U postgres -d osa

# === Image ===

# Print the current image tag (based on git sha)
image-tag:
    @echo "ghcr.io/$(gh repo view --json owner,name -q '.owner.login')/osa:sha-$(git rev-parse --short=7 HEAD)"

# Print the tag of the latest image pushed to GHCR
image-latest:
    @GH_PAGER= gh api /orgs/opensciencearchive/packages/container/osa/versions --jq '.[0].metadata.container.tags[0]'

# === Release ===

# Cut a new release (kind: patch | minor | major). Bumps server/pyproject.toml, pushes to main, creates a GitHub release.
release kind:
    #!/usr/bin/env bash
    set -euo pipefail

    case "{{kind}}" in
        patch|minor|major) ;;
        *) echo "release: kind must be patch, minor, or major (got '{{kind}}')" >&2; exit 1 ;;
    esac

    # Require clean main, in sync with origin — release builds don't gate on CI,
    # so a bad commit will publish a bad image.
    branch="$(git rev-parse --abbrev-ref HEAD)"
    if [ "$branch" != "main" ]; then
        echo "release: must be on 'main' (currently on '$branch')" >&2
        exit 1
    fi
    if ! git diff --quiet HEAD -- ; then
        echo "release: working tree is dirty — commit or stash first" >&2
        exit 1
    fi
    git fetch --quiet origin main
    if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]; then
        echo "release: local main is not in sync with origin/main — pull/push first" >&2
        exit 1
    fi

    # Latest CI on main must be green. Warn-and-confirm rather than hard-block
    # so an unrelated flake doesn't strand the release.
    ci_status="$(gh run list --workflow=ci.yml --branch=main --limit=1 --json conclusion --jq '.[0].conclusion' 2>/dev/null || echo unknown)"
    if [ "$ci_status" != "success" ]; then
        echo "release: latest ci.yml run on main is '${ci_status}' (not success)" >&2
        read -p "release: continue anyway? [y/N] " yn
        case "$yn" in [yY]*) ;; *) echo "release: aborted"; exit 1 ;; esac
    fi

    current="$(awk -F'"' '/^version = / {print $2; exit}' server/pyproject.toml)"
    if [ -z "$current" ]; then
        echo "release: could not read version from server/pyproject.toml" >&2
        exit 1
    fi

    IFS=. read -r maj min pat <<< "$current"
    case "{{kind}}" in
        patch) pat=$((pat + 1)) ;;
        minor) min=$((min + 1)); pat=0 ;;
        major) maj=$((maj + 1)); min=0; pat=0 ;;
    esac
    next="${maj}.${min}.${pat}"
    tag="v${next}"

    echo "release: ${current} -> ${next} (tag ${tag})"
    read -p "release: cut ${tag} from main? [y/N] " yn
    case "$yn" in [yY]*) ;; *) echo "release: aborted"; exit 1 ;; esac

    # -i.bak portable across BSD (macOS) and GNU sed.
    sed -i.bak -e "s/^version = \".*\"/version = \"${next}\"/" server/pyproject.toml
    rm -f server/pyproject.toml.bak

    git add server/pyproject.toml
    git commit -m "chore: bump version to ${next}"
    git push origin main

    gh release create "${tag}" \
        --target main \
        --title "${tag}" \
        --generate-notes

    echo "release: ${tag} cut. Watch the image build with: gh run watch"

# === Maintenance ===

# Clean up Docker resources (volumes, images, etc.)
clean:
    docker compose -f deploy/docker-compose.yml down -v --rmi local

# Show service status
status:
    docker compose -f deploy/docker-compose.yml ps
