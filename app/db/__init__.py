import re
import logging

from slugify import slugify
from funcy import first, is_list, omit
from tinydb import TinyDB, Query, where
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
def add_connection(name, store_type, categories, props, db=None):
    slug = props.get("slug", slugify(name))

    if is_connection_exists(name, slug) == True:
        logger.error(
            f"Connection with the same name <{name}> or slug <{slug}> already exists!"
        )
        exit(1)

    logger.info(f"Adding connection {name} ...")
    connections = db.table("connections")
    connections.insert(
        {
            "name": name,
            "slug": slug,
            "type": store_type,
            "categories": categories,
            "props": omit(props, ["slug"]),
        }
    )
    logger.info(f"✓ Success")


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
    return [
        c
        for c in connections
        if len(list(set(c.get("categories", ["generic"])) & set(categories))) > 0
    ]


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
def get_connection(name, ignore_case=True, db=None):
    Connection = Query()
    connections = db.table("connections")
    if not ignore_case:
        return first(connections.search(Connection.name == name))
    else:
        return first(
            connections.search(Connection.name.matches(name, flags=re.IGNORECASE))
        )


@command
def get_connection_by_slug(slug, ignore_case=True, db=None):
    Connection = Query()
    connections = db.table("connections")
    if not ignore_case:
        return first(connections.search(Connection.slug == slug))
    else:
        return first(
            connections.search(Connection.name.matches(slug, flags=re.IGNORECASE))
        )


@command
def is_connection_exists(name, slug, db=None):
    Connection = Query()
    connections = db.table("connections")
    result = connections.search(
        Connection.name.matches(name, flags=re.IGNORECASE)
        | Connection.name.matches(name, flags=re.IGNORECASE)
    )
    return len(result) > 0


@command
def get_mapping(name, db=None):
    Mapping = Query()
    mappings = db.table("mappings")
    return first(mappings.search(Mapping.name == name))


@command
def remove_connection_by_name_or_slug(name_or_slug, db=None):
    logger.info(f"Removing connection {name_or_slug}")
    connections = db.table("connections")
    connections.remove(
        (where("slug") == name_or_slug) | (where("name") == name_or_slug)
    )
    logger.info("✓ Success")
