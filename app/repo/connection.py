import sys
import re
import logging

from slugify import slugify
from funcy import first, is_list, omit
from tinydb import Query, where
from app.repo import db

logger = logging.getLogger("repository.connection")


__all__ = [
    "get_connections",
    "get_connections_by_type",
    "get_connections_by_category",
    "add_connection",
    "remove_connection_by_name_or_slug"
]

def get_connections():
    connections = db.table("connections")
    return connections.all()


def get_connections_by_type(_type):
    connection = Query()
    connections = db.table("connections")
    return connections.search(connection.type == _type)


def get_connections_by_category(categories):
    if not is_list(categories):
        categories = [categories]

    connections = db.table("connections").all()
    return [
        c
        for c in connections
        if len(list(set(c.get("categories", ["generic"])) & set(categories))) > 0
    ]


def add_connection(name, store_type, categories, props):
    slug = props.get("slug", slugify(name))

    if is_connection_exists(name, slug):
        logger.error(
            "Connection with the same name <%s> or slug <%s> already exists!",
            name,
            slug,
        )
        sys.exit(1)

    logger.info("Adding connection %s...", name)
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
    logger.info("✓ Success")


def get_connection(name, ignore_case=True):
    connection = Query()
    connections = db.table("connections")
    if not ignore_case:
        return first(connections.search(connection.name == name))

    return first(
        connections.search(connection.name.matches(name, flags=re.IGNORECASE))
    )


def get_connection_by_slug(slug, ignore_case=True):
    connection = Query()
    connections = db.table("connections")
    if not ignore_case:
        return first(connections.search(connection.slug == slug))

    return first(
        connections.search(connection.name.matches(slug, flags=re.IGNORECASE))
    )


def is_connection_exists(name, slug):
    connection = Query()
    connections = db.table("connections")
    result = connections.search(
        connection.name.matches(name, flags=re.IGNORECASE)
        | connection.name.matches(slug, flags=re.IGNORECASE)
    )
    return len(result) > 0


def remove_connection_by_name_or_slug(name_or_slug):
    logger.info("Removing connection %s", name_or_slug)
    connections = db.table("connections")
    connections.remove(
        (where("slug") == name_or_slug) | (where("name") == name_or_slug)
    )
    logger.info("✓ Success")
