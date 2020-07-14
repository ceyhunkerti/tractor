import logging
import click
from app import repo
from .add import add
from .remove import remove


@click.group()
def tractor():
    """Tractor"""



@click.command(name="describe", help="Show configuration contents")
def describe():
    repo.describe()


# @click.command(help="Scans connections for indexing items")
# @click.argument('name_or_slug')
# def run(name_or_slug):
#     mapping = repo.get_mapping_by_name_or_slug(name_or_slug)
#     engine_class = registery.get_item_by_key(mapping['engine'])
#     engine = engine_class(mapping['props'])
#     engine.run()

tractor.add_command(add)
tractor.add_command(remove)
# tractor.add_command(scan)
tractor.add_command(describe)
# tractor.add_command(run)
