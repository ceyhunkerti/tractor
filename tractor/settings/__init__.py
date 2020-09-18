import os
from funcy import distinct, remove
from .helpers import parse_boolean, array_from_string

LOG_LEVEL = os.environ.get("TRACTOR_LOG_LEVEL", "INFO")
LOG_STDOUT = parse_boolean(os.environ.get("TRACTOR_LOG_STDOUT", "false"))
LOG_PREFIX = os.environ.get("TRACTOR_LOG_PREFIX", "")
LOG_FORMAT = os.environ.get(
    "TRACTOR_LOG_FORMAT",
    LOG_PREFIX + "[%(asctime)s][PID:%(process)d][%(levelname)s][%(name)s] %(message)s",
)


default_plugins = [
    "tractor.plugins.input",
    "tractor.plugins.output",
]
enabled_plugins = array_from_string(
    os.environ.get("TRACTOR_ENABLED_PLUGINS", ",".join(default_plugins))
)
additional_plugins = array_from_string(
    os.environ.get("TRACTOR_ADDITIONAL_PLUGINS", "")
)
disabled_plugins = array_from_string(
    os.environ.get("TRACTOR_DISABLED_PLUGINS", "")
)
PLUGINS = remove(
    set(disabled_plugins),
    distinct(enabled_plugins + additional_plugins),
)

CONFIG_FILE = os.environ.get("TRACTOR_CONFIG_FILE", "./tractor.yml")


META_CHANNEL_TIMEOUT = int(os.environ.get('TRACTOR_META_CHANNEL_TIMEOUT', 10))
DATA_CHANNEL_TIMEOUT = int(os.environ.get('TRACTOR_DATA_CHANNEL_TIMEOUT', 20))
CHANNEL_TIMEOUT = int(os.environ.get('TRACTOR_CHANNEL_TIMEOUT', 20))
COUNT_CHANNEL_TIMEOUT = int(os.environ.get('TRACTOR_COUNT_CHANNEL_TIMEOUT', 60))

YAML_FILE_ENCODING = os.environ.get('TRACTOR_YAML_FILE_ENCODING', "utf-8")
