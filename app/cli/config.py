import logging
import click
import questionary as q

from app import db
from app.engine import engines, get_engine
from app import settings

logger = logging.getLogger("config")


def get_engine_types():
    return {e.name(): e.type() for _, e in engines.items()}


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
    engine_types = get_engine_types()
    et = q.select("Select connection type", choices=engine_types.keys()).ask()

    engine_type = engine_types.get(et)
    engine = get_engine(engine_type)
    props = engine.ask_connection()
    db.add_connection(props["name"], engine_type, props)


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
    engine = get_engine(source_conn_type)
    source_props = engine.ask_mapping_source()

    print("== Mapping Target Properties ==")
    engine = get_engine(target_conn_type)
    target_props = engine.ask_mapping_target()

    db.add_mapping(
        name, source_connection, target_connection, source_props, target_props
    )

