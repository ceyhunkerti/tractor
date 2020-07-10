import logging
import click
from app import repo

logger = logging.getLogger("config.connection")

@click.command(help="Scans connections for indexing items")
@click.argument('connection_name')
def scan(connection_name):
    repo.scan(connection_name)
