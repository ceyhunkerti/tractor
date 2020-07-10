import logging
import sys

from funcy import first
from tinydb import Query
from app.repo import db

logger = logging.getLogger("repository.mapping")

__all__ = [
    "add_mapping",
    "get_mapping"
]


def add_mapping(name, source_connection, target_connection, reader_conf, writer_conf):
    if get_mapping(name) is not None:
        logger.error("Mapping with the same name <%s> exists", name)
        sys.exit(1)

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


def get_mapping(name):
    mapping = Query()
    mappings = db.table("mappings")
    return first(mappings.search(mapping.name == name))
