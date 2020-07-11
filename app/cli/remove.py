import click

from app import repo

@click.group()
def remove():
    """Delete config commands"""

@remove.command(name="connection", help="Delete connection")
@click.argument("name_or_slug")
def connection(name_or_slug):
    repo.remove_connection_by_name_or_slug(name_or_slug)
