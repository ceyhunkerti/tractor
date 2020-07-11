import logging
import click
import questionary as q
from funcy import omit

from app import repo
from app.store import registery as store_registery
from app.engine import registery as engine_registery



logger = logging.getLogger("config.connection")

@click.group()
def add():
    """Delete config commands"""


@add.command(name="connection", help="Add new connection")
def connection():
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



@add.command(name="mapping", help="Add new mapping")
def mapping():

    name = q.text("Enter mapping name:").ask()
    engine_name = q.select("Select engine", choices=engine_registery.names).ask()
    engine = engine_registery.get_item_by_name(engine_name)
    props = engine.ask()

    repo.add_mapping(name, engine.type(), props)
