import questionary
from .base import BaseEngine
from . import register

try:
    import cx_Oracle

    enabled = True
except ImportError:
    enabled = False


class Csv(BaseEngine):
    connection_questions = BaseEngine.connection_questions + [
        {"type": "text", "name": "path", "message": "Path to folder:",},
        {
            "type": "text",
            "default": ",",
            "name": "field_delimiter",
            "message": "Field delimiter:",
        },
        {
            "type": "text",
            "default": "\\n",
            "name": "record_delimiter",
            "message": "Record delimiter:",
        },
        {
            "type": "text",
            "name": "skip",
            "message": "Skip header:",
            "default": "0",
            "validate": lambda v: v.isdigit(),
            "filter": lambda v: int(v)
        },
    ]

    mapping_source_questions = BaseEngine.connection_questions + [
        {
            "type": "text",
            "name": "file_name",
            "message": "File name:",
        },
        {
            "type": "select",
            "name": "use_defaults",
            "message": "Use connection defaults (eg. delimiters, skip, etc.)",
            "choices": ["Yes", "No"]
        },
        {
            "when": lambda x: x['use_defaults'] == 'No',
            "type": "text",
            "default": ",",
            "name": "field_delimiter",
            "message": "Field delimiter:",
        },
        {
            "when": lambda x: x['use_defaults'] == 'No',
            "type": "text",
            "default": "\\n",
            "name": "record_delimiter",
            "message": "Record delimiter:",
        },
        {
            "when": lambda x: x['use_defaults'] == 'No',
            "type": "text",
            "name": "skip",
            "message": "Skip header:",
            "default": "0",
            "validate": lambda v: v.isdigit(),
            "filter": lambda v: int(v)
        },
    ]

    mapping_target_questions = BaseEngine.connection_questions + [
        {
            "type": "text",
            "name": "file_name",
            "message": "File name:",
        },
        {
            "type": "select",
            "name": "use_defaults",
            "message": "Use connection defaults (eg. delimiters, skip, etc.)",
            "choices": ["Yes", "No"]
        },
        {
            "when": lambda x: x['use_defaults'] == 'No',
            "type": "text",
            "default": ",",
            "name": "field_delimiter",
            "message": "Field delimiter:",
        },
        {
            "when": lambda x: x['use_defaults'] == 'No',
            "type": "text",
            "default": "\\n",
            "name": "record_delimiter",
            "message": "Record delimiter:",
        },
        {
            "when": lambda x: x['use_defaults'] == 'No',
            "type": "text",
            "name": "skip",
            "message": "Skip header:",
            "default": "0",
            "validate": lambda v: v.isdigit(),
            "filter": lambda v: int(v)
        },
    ]

    @classmethod
    def name(cls):
        return "CSV"

    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def type(cls):
        return "csv"

    def __init__(self, channel, config):
        super(Csv, self).__init__(channel, config)


register(Csv)
