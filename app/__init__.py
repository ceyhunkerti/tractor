import os
import logging
import sys
from .cli import dumper
from . import settings
from app.store import import_stores
from app.engine import import_engines

def setup_logging():
    handler = logging.StreamHandler(sys.stdout if settings.LOG_STDOUT else sys.stderr)
    formatter = logging.Formatter(settings.LOG_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(settings.LOG_LEVEL)

setup_logging()

import_stores(settings.STORES)
import_engines(settings.ENGINES)