"""Main CLI application."""

import typer

from osa.cli.commands import search, server

app = typer.Typer(
    name="osa",
    help="Open Science Archive - CLI",
    no_args_is_help=True,
)

app.add_typer(server.app, name="server")
app.add_typer(search.app, name="search")
