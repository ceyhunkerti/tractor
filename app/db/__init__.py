import logging

from funcy import first
from tinydb import TinyDB, Query
from .storage.yaml_storage import YAMLStorage


# todo move to settings
DB_PATH = "config/app.yml"
logger = logging.getLogger("db")

def command(func):
    def wrapper(*args, **kwarg):
        result = None
        error = None
        with TinyDB(DB_PATH, storage=YAMLStorage) as db:
            try:
                result = func(*args, **kwarg, db=db)
            except Exception as e:
                error = e
                logger.error("Repository exception")
        if error is not None:
            raise error
        return result
    return wrapper


@command
def describe(db=None):
    logger.info("Listing repositroy contents ...")
    connections = db.table('connections')
    print(f"{len(connections)} connections")


@command
def add_connection(name, engine_type, props, db=None):
    if get_connection(name) is not None:
        logger.error(f"Connection with the same name <{name}> exists")
        exit(1)

    logger.info(f"Adding connection {name} ...")
    connections = db.table('connections')
    connections.insert({
        "name": name,
        "type": engine_type,
        "props": props
    })

@command
def get_connections(db=None):
    connections = db.table('connections')
    return connections.all()

@command
def add_mapping(name, source_connection, target_connection, source_props, target_props, db=None):
    if get_mapping(name) is not None:
        logger.error(f"Mapping with the same name <{name}> exists")
        exit(1)

    mappings = db.table('mappings')
    mappings.insert({
        "name": name,
        "source_connection": source_connection,
        "target_connection": target_connection,
        "source_props": source_props,
        "target_props": target_props,
    })

@command
def get_connection(name, db=None):
    Connection = Query()
    connections = db.table('connections')
    return first(connections.search(Connection.name == name))

@command
def get_mapping(name, db=None):
    Mapping = Query()
    mappings = db.table("mappings")
    return first(mappings.search(Mapping.name == name))
