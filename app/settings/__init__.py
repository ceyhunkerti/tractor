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

default_engines = [
    "app.engine.oracle",
    "app.engine.csv",
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
