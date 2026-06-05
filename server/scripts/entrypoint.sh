#!/bin/bash
# Server entrypoint: wait for Postgres, run migrations, optionally seed the
# dev admin (OSA_DEV_MODE=true), then hand off to the container CMD.
#
# Used by both the published image (Dockerfile ENTRYPOINT) and `osa start
# --source` (dev override). The actual uvicorn invocation lives in CMD so
# that production runs without `--reload` and the dev override can opt into
# reload by overriding CMD.

set -euo pipefail

VENV_BIN="/app/.venv/bin"

# ---------------------------------------------------------------------------
# Wait for Postgres readiness
# ---------------------------------------------------------------------------
# Parse host/port out of OSA_DATABASE__URL to avoid hard-coding "db:5432".
# Supports postgresql[+driver]://user:pass@host:port/db.
DB_URL="${OSA_DATABASE__URL:-}"
if [[ -z "${DB_URL}" ]]; then
    echo "entrypoint: OSA_DATABASE__URL is not set" >&2
    exit 1
fi

DB_HOSTPORT="${DB_URL#*@}"
DB_HOSTPORT="${DB_HOSTPORT%%/*}"
DB_HOST="${DB_HOSTPORT%%:*}"
DB_PORT="${DB_HOSTPORT##*:}"
if [[ "${DB_PORT}" == "${DB_HOST}" ]]; then
    DB_PORT="5432"
fi

echo "entrypoint: waiting for Postgres at ${DB_HOST}:${DB_PORT}..."
ATTEMPTS=0
MAX_ATTEMPTS=60
until "${VENV_BIN}/python" -c "
import socket, sys
s = socket.socket(); s.settimeout(1)
try:
    s.connect(('${DB_HOST}', ${DB_PORT}))
    sys.exit(0)
except OSError:
    sys.exit(1)
finally:
    s.close()
" 2>/dev/null; do
    ATTEMPTS=$((ATTEMPTS + 1))
    if [[ ${ATTEMPTS} -ge ${MAX_ATTEMPTS} ]]; then
        echo "entrypoint: Postgres did not become reachable after ${MAX_ATTEMPTS}s" >&2
        exit 1
    fi
    sleep 1
done
echo "entrypoint: Postgres is reachable"

# ---------------------------------------------------------------------------
# Migrations + (optional) dev admin seed
# ---------------------------------------------------------------------------
echo "entrypoint: running alembic upgrade head"
"${VENV_BIN}/alembic" upgrade head

if [[ "${OSA_DEV_MODE:-false}" == "true" ]]; then
    echo "entrypoint: OSA_DEV_MODE=true — seeding dev admin"
    "${VENV_BIN}/python" /app/scripts/seed_dev_admin.py
fi

# ---------------------------------------------------------------------------
# Hand off to the container CMD (uvicorn) as PID 1
# ---------------------------------------------------------------------------
echo "entrypoint: exec $*"
exec "$@"
