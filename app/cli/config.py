import logging
import click
import questionary as q

from app import db
from app.store import stores, get_store
from app import settings

logger = logging.getLogger("config")


def get_store_types():
    return {e.name(): e.type() for _, e in stores.items()}


# Command Group
@click.group()
def config():
    """Repository commands"""
    pass


@config.command(name="describe", help="Show configuration contents")
def describe():
    db.describe()


@config.group()
def add():
    """Repository commands"""
    pass


@add.command(name="connection", help="Add new connection")
def add_connection():
    store_types = get_store_types()
    et = q.select("Select connection type", choices=store_types.keys()).ask()

    store_type = store_types.get(et)
    store = get_store(store_type)
    props = store.ask_connection()
    db.add_connection(props["name"], store_type, props)


@add.command(name="mapping", help="Add new mapping")
def add_mapping():
    connections = db.get_connections()

    if len(connections) == 0:
        logger.info(
            """
            Create connections first.
            You can run the following command to create connection.

            dumper config add connection
        """
        )
        exit(1)

    name = q.text(
        "Mapping name:", validate=lambda x: x is not None or x.strip() != ""
    ).ask()

    source_connection = q.select(
        "Select source connection", choices=[c["name"] for c in connections]
    ).ask()
    source_conn_type = db.get_connection(source_connection)["type"]

    target_connection = q.select(
        "Select target connection", choices=[c["name"] for c in connections]
    ).ask()
    target_conn_type = db.get_connection(target_connection)["type"]

    print("== Mapping Source Properties ==")
    store = get_store(source_conn_type)
    reader_conf = store.ask_reader()

    print("== Mapping Target Properties ==")
    store = get_store(target_conn_type)
    writer_conf = store.ask_writer()

    db.add_mapping(
        name, source_connection, target_connection, reader_conf, writer_conf
    )

