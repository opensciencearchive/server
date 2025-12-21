"""Main CLI application using Cyclopts.

The CLI is a thin HTTP client - it talks to the server via REST API.
No internal DI needed since all business logic lives in the server.
"""

import cyclopts

from osa.cli.commands import admin, config, search, server

app = cyclopts.App(
    name="osa",
    help="Open Science Archive - CLI",
)

app.command(server.app, name="server")
app.command(search.app, name="search")
app.command(config.app, name="config")
app.command(admin.app, name="admin")
