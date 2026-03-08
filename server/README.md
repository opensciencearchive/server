# OSA Server

Backend for Open Science Archive.

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
