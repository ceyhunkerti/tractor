import click

from .config import add, remove
from app import settings

@click.group()
def tractor():
    """Tractor"""

tractor.add_command(add)
tractor.add_command(remove)