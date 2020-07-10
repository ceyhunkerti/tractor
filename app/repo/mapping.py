import logging
import sys

from slugify import slugify
from funcy import first
from tinydb import Query
from app.repo import db

logger = logging.getLogger("repository.mapping")

__all__ = [
    "add_mapping",
    "get_mapping"
]


def add_mapping(name, engine, props):
    if get_mapping(name) is not None:
        logger.error("Mapping with the same name <%s> exists", name)
        sys.exit(1)

    logger.info("Adding mapping...")
    mappings = db.table("mappings")
    mappings.insert(
        {
            "name": name,
            "slug": slugify(name),
            "engine": engine,
            "props": props
        }
    )
    logger.info("✓ Success")

def get_mapping(name):
    mapping = Query()
    mappings = db.table("mappings")
    return first(mappings.search(mapping.name == name))
