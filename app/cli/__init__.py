import click

from .config import config
from app import settings

@click.group()
def dumper():
    """Dumper"""

dumper.add_command(config)