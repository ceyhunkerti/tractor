import logging

logger = logging.getLogger("util.registery")

class Registery:
    def __init__(self):
        self.registery = dict()

    def register(self, item):
        if item.enabled():
            logger.debug(
                "Registering %s (%s) item.", item.name(), item.type(),
            )
            self.registery[item.type()] = item
        else:
            logger.debug(
                """%s item enabled but not supported, not registering.
                Either disable or install missing dependencies.""",
                item.name(),
            )

    def get_item_by_key(self, key):
        return self.registery.get(key, None)

    def get_item_by_name(self, name):
        key = self._get_key_by_name(name)
        return self.get_item_by_key(key)

    def _get_key_by_name(self, name):
        return self._name_key_map.get(name)

    @property
    def _name_key_map(self):
        return {s.name(): s.type() for _, s in self.registery.items()}

    @property
    def names(self):
        return [s.name() for _, s in self.registery.items()]

    @property
    def keys(self):
        return self.registery.keys()
