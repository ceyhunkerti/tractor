import os
import logging
import sys
from tractor.plugins import import_plugins
from .cli import tractor
from . import settings

def setup_logging():
    handler = logging.StreamHandler(sys.stdout if settings.LOG_STDOUT else sys.stderr)
    formatter = logging.Formatter(settings.LOG_FORMAT)
    handler.setFormatter(formatter)
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(settings.LOG_LEVEL)

setup_logging()

import_plugins(settings.PLUGINS)
