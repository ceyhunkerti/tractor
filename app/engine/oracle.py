import questionary
from .base import BaseEngine
from . import register

try:
    import cx_Oracle

    enabled = True
except ImportError:
    enabled = False


class Oracle(BaseEngine):
    connection_questions = BaseEngine.connection_questions + [
        {"type": "text", "name": "host", "message": "Host name or ip address:",},
        {
            "type": "text",
            "default": "1521",
            "name": "port",
            "message": "Port number",
            "validate": lambda val: val.isdigit() and int(val) in range(1, 65535),
            "filter": lambda val: int(val),
        },
        {"type": "text", "name": "service", "message": "Service name or SID:",},
    ]

    mapping_source_questions = BaseEngine.mapping_source_questions + [
        {"type": "text", "name": "table", "message": "Source table [schema.table or just table]:",},
        {"type": "text", "name": "hint", "message": "Select hint:",},
        {
            "type": "text",
            "name": "fetch_size",
            "message": "Fetch size:",
            "default": "1000",
            "validate": lambda v: v.isdigit(),
            "filter": lambda v: int(v)
        },
    ]

    mapping_target_questions = BaseEngine.mapping_target_questions + [
        {"type": "text", "name": "table", "message": "Target table [schema.table or just table]:",},
        {
            "type": "text",
            "name": "batch_size",
            "message": "Batch size:",
            "default": "1000",
            "validate": lambda v: v.isdigit(),
            "filter": lambda v: int(v)
        },
        {"type": "text", "name": "hint", "message": "Insert hint:",},
    ]

    @classmethod
    def name(cls):
        return "Oracle"

    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def type(cls):
        return "oracle"

    def __init__(self, *args, **kwargs):
        super(Oracle, self).__init__()


register(Oracle)
