import logging
from .base import repo

logger = logging.getLogger("repository.mapping")

__all__ = ["add_mapping", "get_mapping", "get_mapping_names"]


def add_mapping(name, options):
    config = repo.read()
    mapping = {}
    mapping[name] = options
    config["mappings"].append(mapping)
    repo.write(config)


def get_mapping(name):
    config = repo.read()
    try:
        for mapping in config["mappings"]:
            key = list(mapping.keys())[0]
            if key.lower() == name.lower():
                return mapping[key]
    except IndexError:
        return None

    return None


def get_mapping_names():
    config = repo.read()
    mappings = config.get("mappings")
    return sum([list(m.keys()) for m in mappings], [])
