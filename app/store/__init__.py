import logging
from app.util import Registery

logger = logging.getLogger("store")


def import_stores(stores):
    for store in stores:
        __import__(store)



registery = Registery()
