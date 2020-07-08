import logging

logger = logging.getLogger("store")


def import_stores(stores):
    for store in stores:
        __import__(store)


stores = {}

def register(store):
    global stores
    if store.enabled():
        logger.debug(
            "Registering %s (%s) store.", store.name(), store.type(),
        )
        stores[store.type()] = store
    else:
        logger.debug(
            "%s store enabled but not supported, not registering. Either disable or install missing "
            "dependencies.",
            store.name(),
        )

def get_store(store_type):
    store_class = stores.get(store_type, None)
    if store_class is None:
        return None

    return store_class()