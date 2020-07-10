import logging

logger = logging.getLogger("store")


def import_stores(stores):
    for store in stores:
        __import__(store)


class Registery:
    def __init__(self):
        self.registery = dict()

    def register(self, store):
        if store.enabled():
            logger.debug(
                "Registering %s (%s) store.", store.name(), store.type(),
            )
            self.registery[store.type()] = store
        else:
            logger.debug(
                """%s store enabled but not supported, not registering.
                Either disable or install missing dependencies.""",
                store.name(),
            )

    def get_store(self, store_type):
        return self.registery.get(store_type, None)

registery = Registery()
