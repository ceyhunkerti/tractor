import questionary as q
import click

from tractor.util import required
from tractor import repo

@click.group()
def remove():
    """Delete config commands"""


@remove.command(name="mapping", help="Delete mapping")
@click.argument("name", default=None, required=False)
def mapping(name):
    if not name:
        name = q.autocomplete(
            "* Enter mapping name", choices=repo.get_mapping_names(), validate=required
        ).ask()

    repo.delete_mapping(name)
