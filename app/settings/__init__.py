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
    "app.plugins.input",
    "app.plugins.output",
    "app.plugins.solo",
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


# default csv params if no delimiter is given for mappings and connections
CSV_FIELD_DELIMITER = os.environ.get("TRACTOR_CSV_FIELD_DELIMITER", ",")
CSV_RECORD_DELIMITER = os.environ.get("TRACTOR_CSV_RECORD_DELIMITER", "\\n")
CSV_PATH = os.environ.get("TRACTOR_CSV_PATH", "/tmp")
