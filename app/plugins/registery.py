import logging
from typing import List
from .base import PluginTypes, BasePlugin

logger = logging.getLogger("plugins.registery")


class Registery:
    def __init__(self):
        self.registery = dict(input={}, output={}, filter={}, solo={})

    def register(self, item):
        if item.enabled():
            logger.debug("Registering %s item.", item.slug())
            if self.registery[item.type().value].get(item.name()) is not None:
                raise ValueError(
                    f"""Name {item.name()} with plugin type {item.type().value} already registered.
                        Plugins names must be unique per plugin-type
                    """
                )
            self.registery[item.type().value][item.name()] = item
        else:
            logger.debug(
                """%s item enabled but not supported, not registering.
                Either disable or install missing dependencies.""",
                item.name(),
            )

    def get_item(self, plugin_type: PluginTypes, name) -> BasePlugin:
        return self.registery.get(plugin_type.value).get(name)

    def items(self, plugin_type: PluginTypes) -> List[BasePlugin]:
        return self.registery[plugin_type.value].values()
