import logging
from app.util import Registery

logger = logging.getLogger("engine")


def import_engines(engines):
    for engine in engines:
        __import__(engine)


registery = Registery()
