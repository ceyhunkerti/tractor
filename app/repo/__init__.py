import re
import logging

from slugify import slugify
from funcy import first, is_list, omit
from tinydb import TinyDB, Query, where
from .base import db
from .connection import *
from .mapping import *

logger = logging.getLogger("repo")

def describe():
    logger.info("Listing repositroy contents ...")
    connections = db.table("connections")
    print(f"{len(connections)} connections")
