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
            "%s store enabled but not supported, not registering. Either disable or install missing "
            "dependencies.",
            engine.name(),
        )

def get_engine(engine_type):
    return engines.get(engine_type, None)
