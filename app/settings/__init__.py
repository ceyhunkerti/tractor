import os
from funcy import distinct, remove
from .helpers import parse_boolean, array_from_string

LOG_LEVEL = os.environ.get("DUMPER_LOG_LEVEL", "INFO")
LOG_STDOUT = parse_boolean(os.environ.get("DUMPER_LOG_STDOUT", "false"))
LOG_PREFIX = os.environ.get("DUMPER_LOG_PREFIX", "")
LOG_FORMAT = os.environ.get(
    "DUMPER_LOG_FORMAT",
    LOG_PREFIX + "[%(asctime)s][PID:%(process)d][%(levelname)s][%(name)s] %(message)s",
)

default_stores = [
    "app.store.oracle",
    "app.store.csv",
]
enabled_stores = array_from_string(
    os.environ.get("DUMPER_ENABLED_STORES", ",".join(default_stores))
)
additional_stores = array_from_string(
    os.environ.get("DUMPER_ADDITIONAL_STORES", "")
)
disabled_stores = array_from_string(
    os.environ.get("DUMPER_DISABLED_STORES", "")
)
STORES = remove(
    set(disabled_stores),
    distinct(enabled_stores + additional_stores),
)


default_engines = [
    "app.engine.oracle_dblink",
    "app.engine.sql",
]
enabled_engines = array_from_string(
    os.environ.get("DUMPER_ENABLED_ENGINES", ",".join(default_engines))
)
additional_engines = array_from_string(
    os.environ.get("DUMPER_ADDITIONAL_ENGINES", "")
)
disabled_engines = array_from_string(
    os.environ.get("DUMPER_DISABLED_ENGINES", "")
)
ENGINES = remove(
    set(disabled_engines),
    distinct(enabled_engines + additional_engines),
)


# default csv params if no delimiter is given for mappings and connections
CSV_FIELD_DELIMITER = os.environ.get("DUMPER_CSV_FIELD_DELIMITER", ",")
CSV_RECORD_DELIMITER = os.environ.get("DUMPER_CSV_RECORD_DELIMITER", "\\n")
CSV_PATH = os.environ.get("DUMPER_CSV_PATH", "/tmp")