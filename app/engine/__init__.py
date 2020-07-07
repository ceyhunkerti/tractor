import logging

logger = logging.getLogger("engine")


def import_engines(engines):
    for engine in engines:
        __import__(engine)


engines = {}


def register(engine):
    global engines
    if engine.enabled():
        logger.debug(
            "Registering %s (%s) engine.", engine.name(), engine.type(),
        )
        engines[engine.type()] = engine
    else:
        logger.debug(
            "%s engine enabled but not supported, not registering. Either disable or install missing "
            "dependencies.",
            engine.name(),
        )

def get_engine(engine_type):
    engine_class = engines.get(engine_type, None)
    if engine_class is None:
        return None

    return engine_class()