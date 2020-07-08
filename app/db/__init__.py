import logging

from funcy import first, is_list
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
                logger.error(f"{func.__name__}: Repository exception")
        if error is not None:
            raise error
        return result

    return wrapper


@command
def describe(db=None):
    logger.info("Listing repositroy contents ...")
    connections = db.table("connections")
    print(f"{len(connections)} connections")


@command
def add_connection(name, store_type, props, db=None):
    if get_connection(name) is not None:
        logger.error(f"Connection with the same name <{name}> exists")
        exit(1)

    logger.info(f"Adding connection {name} ...")
    connections = db.table("connections")
    connections.insert({"name": name, "type": store_type, "props": props})


@command
def get_connections(db=None):
    connections = db.table("connections")
    return connections.all()


@command
def get_connections_by_type(_type, db=None):
    Connection = Query()
    connections = db.table("connections")
    return connections.search(Connection.type == _type)


@command
def get_connections_by_category(categories, db=None):
    if not is_list(categories):
        categories = [categories]

    connections = db.table("connections").all()
    return [c for c in connections if list(set(c.categories) & set(categories)) > 0]


@command
def add_mapping(
    name, source_connection, target_connection, reader_conf, writer_conf, db=None
):
    if get_mapping(name) is not None:
        logger.error(f"Mapping with the same name <{name}> exists")
        exit(1)

    mappings = db.table("mappings")
    mappings.insert(
        {
            "name": name,
            "source_connection": source_connection,
            "target_connection": target_connection,
            "reader_conf": reader_conf,
            "writer_conf": writer_conf,
        }
    )


@command
def get_connection(name, db=None):
    Connection = Query()
    connections = db.table("connections")
    return first(connections.search(Connection.name == name))


@command
def get_mapping(name, db=None):
    Mapping = Query()
    mappings = db.table("mappings")
    return first(mappings.search(Mapping.name == name))
