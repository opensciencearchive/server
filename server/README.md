# OSA Server

Python backend for Open Science Archive, built with FastAPI.

## Development

```bash
# Install dependencies
uv sync

# Run development server
just dev

# Run tests
just test

# Lint and type check
just lint
```

## Structure

```
server/
├── osa/           # Application code
├── tests/         # Test suite
├── migrations/    # Database migrations (Alembic)
├── sources/       # Data source plugins
├── Dockerfile     # Container build
└── Justfile       # Development commands
```

See the [root README](../README.md) for full documentation.
