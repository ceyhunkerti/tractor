import logging
import click
import questionary as q
from funcy import omit

from app import repo
from app.engine import engines, get_engine
from app.store import stores, get_store

logger = logging.getLogger("config")


def get_store_types():
    return {s.name(): s.type() for _, s in stores.items()}


def get_engine_types():
    return {e.name(): e.type() for _, e in engines.items()}


@click.command(name="describe", help="Show configuration contents")
def describe():
    repo.describe()


@click.group()
def add():
    """Add config commands"""

@click.group()
def remove():
    """Delete config commands"""


@add.command(name="connection", help="Add new connection")
def add_connection():
    store_types = get_store_types()
    store_name = q.select("Select connection type", choices=store_types.keys()).ask()

    store_type = store_types.get(store_name)
    store = get_store(store_type)
    props = store.ask()
    repo.add_connection(props["name"], store_type, store.categories, omit(props, ['name']))


@remove.command(name="connection", help="Delete connection")
@click.argument('name_or_slug')
def delete_connection(name_or_slug):
    repo.remove_connection_by_name_or_slug(name_or_slug)



@add.command(name="mapping", help="Add new mapping")
def add_mapping():
    engine_types = get_engine_types()
    engine_name = q.select("Select engine", choices=engine_types.keys()).ask()
    engine_type = engine_types.get(engine_name)
    engine = get_engine(engine_type)
    props = engine.ask()
    repo.add_mapping(props['name'], props)