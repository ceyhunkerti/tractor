import logging

logger = logging.getLogger("engine")


def import_engines(engines):
    for engine in engines:
        __import__(engine)


class Registery:
    def __init__(self):
        self.registery = dict()

    def register(self, engine):
        if engine.enabled():
            logger.debug(
                "Registering %s (%s) engine.", engine.name(), engine.type(),
            )
            self.registery[engine.type()] = engine
        else:
            logger.debug(
                """%s store enabled but not supported, not registering.
                Either disable or install missing dependencies.""",
                engine.name(),
            )

    def get_engine(self, engine_type):
        return self.registery.get(engine_type, None)

registery = Registery()
