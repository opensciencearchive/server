<p align="center">
  <img src="https://opensciencearchive.org/osa_logo.svg" alt="OSA Logo" width="120" />
</p>

<h1 align="center">Open Science Archive</h1>

<p align="center">
  <strong>A domain-agnostic archive for AI-ready scientific data</strong>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> •
  <a href="#development">Development</a> •
  <a href="#project-structure">Structure</a> •
  <a href="#troubleshooting">Troubleshooting</a>
</p>

---

OSA makes it easy to stand up [PDB](https://www.rcsb.org/)-level data infrastructure for any scientific domain — validated, searchable, and AI-ready.

## Quick Start

### Self-Hosted Deployment

Deploy the complete OSA stack with a single command:

**Requirements:** Docker Desktop 4.x+ or Docker Engine 24.x+

```bash
git clone https://github.com/opensciencearchive/server.git
cd server
docker compose up
```

Access the web interface at `http://localhost:8080`

### Environment Configuration

Copy and customize the environment template:

```bash
cp .env.example .env
```

Key variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_PASSWORD` | `osa` | Database password |
| `WEB_PORT` | `8080` | External port for web interface |
| `LOG_LEVEL` | `INFO` | Application log level |

## Development

### Full-Stack Development

Start all services with hot-reload enabled:

```bash
just dev
```

Or using docker compose directly:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up
```

**What's running:**
- Web UI: http://localhost:3000 (hot-reload)
- API Server: http://localhost:8000 (auto-restart on changes)
- PostgreSQL: localhost:5432

### Individual Service Development

**Frontend only:**

```bash
just web-dev
# Or: cd web && pnpm dev
```

**Backend only:**

```bash
just server-dev
# Or: cd server && just dev
```

### Available Commands

| Command | Description |
|---------|-------------|
| `just up` | Start production deployment |
| `just down` | Stop all services |
| `just logs` | View service logs |
| `just dev` | Full-stack development with hot-reload |
| `just web-dev` | Frontend development server |
| `just web-build` | Production build of frontend |
| `just web-lint` | Lint frontend code |
| `just server-dev` | Backend development server |
| `just status` | Show service status |

## Project Structure

```
osa/
├── server/                  # Python backend (FastAPI)
│   ├── osa/                 # Application code
│   ├── tests/               # Test suite
│   ├── migrations/          # Database migrations
│   ├── sources/             # Data source plugins
│   ├── Dockerfile
│   ├── pyproject.toml
│   └── Justfile             # Server-specific commands
├── web/                     # Next.js frontend
│   ├── src/                 # Application code
│   ├── public/              # Static assets
│   ├── Dockerfile
│   └── package.json
├── docker-compose.yml       # Production orchestration
├── docker-compose.dev.yml   # Development overrides
├── Justfile                 # Root orchestration commands
└── .env.example             # Environment template
```

## Troubleshooting

### Port conflicts

If port 8080 is already in use:

```bash
# Option 1: Change the port in .env
echo "WEB_PORT=3001" >> .env
docker compose up

# Option 2: Stop the conflicting service
lsof -i :8080
```

### Database connection issues

Ensure the database container is healthy before starting other services:

```bash
docker compose ps
```

The `db` service should show `healthy` status. If not:

```bash
# Check database logs
docker compose logs db

# Restart just the database
docker compose restart db
```

### Container build failures

If builds fail, try cleaning and rebuilding:

```bash
# Clean up Docker resources
just clean

# Rebuild from scratch
docker compose build --no-cache
docker compose up
```

### Hot-reload not working

For WSL2 users, hot-reload should work automatically (WATCHFILES_FORCE_POLLING is enabled). If it doesn't:

```bash
# Restart the dev environment
docker compose -f docker-compose.yml -f docker-compose.dev.yml down
docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build
```

## License

Apache 2.0
