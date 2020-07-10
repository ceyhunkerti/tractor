import re
import logging

from slugify import slugify
from funcy import first, is_list, omit
from tinydb import TinyDB, Query, where
from .storage.yaml_storage import YAMLStorage
from .connection import *
from .mapping import *

# todo move to settings
DB_PATH = "config/app.yml"
logger = logging.getLogger("repository")

class Repository(TinyDB):
    pass

db = Repository(DB_PATH, storage=YAMLStorage)

def describe():
    logger.info("Listing repositroy contents ...")
    connections = db.table("connections")
    print(f"{len(connections)} connections")
