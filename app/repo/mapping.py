import re
import logging
import sys

from slugify import slugify
from funcy import first
from tinydb import Query, where
from app.repo import db

logger = logging.getLogger("repository.mapping")

__all__ = [
    "add_mapping",
    "get_mapping",
    "get_mapping_by_name_or_slug"
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

def get_mapping_by_name_or_slug(name_or_slug, ignore_case=True):
    mapping = Query()
    mappings = db.table("mappings")
    if ignore_case:
        result = mappings.search(
            mapping.name.matches(name_or_slug, flags=re.IGNORECASE) |
            mapping.slug.matches(name_or_slug, flags=re.IGNORECASE)
        )
    else:
        result = mappings.search(
            (where("slug") == name_or_slug) | (where("name") == name_or_slug))

    return first(result)
