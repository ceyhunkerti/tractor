import logging
import click
import questionary as q
from funcy import omit

from app import repo
from app.engine import registery as engine_registery
from app.store import registery as store_registery

logger = logging.getLogger("config")


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
    store_name = q.select("Select connection type", choices=store_registery.names).ask()

    store = store_registery.get_item_by_name(store_name)
    props = store.ask()
    repo.add_connection(
        props["name"],
        store.type(),
        store.categories,
        omit(props, ["name"]),
        settings=store.settings,
    )


@remove.command(name="connection", help="Delete connection")
@click.argument("name_or_slug")
def delete_connection(name_or_slug):
    repo.remove_connection_by_name_or_slug(name_or_slug)


@add.command(name="mapping", help="Add new mapping")
def add_mapping():

    name = q.text("Enter mapping name:").ask()
    engine_name = q.select("Select engine", choices=engine_registery.names).ask()
    engine = engine_registery.get_item_by_name(engine_name)
    props = engine.ask()

    repo.add_mapping(name, engine.type(), props)
