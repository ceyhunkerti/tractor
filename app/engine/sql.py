import questionary as q

from app import db
from .base import BaseEngine

_reader_questions = [
    {
        "type": "text",
        "name": "table",
        "message": "Source table [schema.table or just table]:",
    },
    {
        "type": "text",
        "name": "columns",
        "message": "Column names, expressions, * for all",
        "default": "*",
    },
    {"type": "text", "name": "hint", "message": "Select hint:",},
    {
        "type": "text",
        "name": "fetch_size",
        "message": "Fetch size:",
        "default": "1000",
        "validate": lambda v: v.isdigit(),
        "filter": lambda v: int(v),
    },
    {"type": "text", "name": "filter", "message": "Filter expression:",},
]

_writer_questions = [
    {
        "type": "text",
        "name": "table",
        "message": "Target table [schema.table or just table]:",
    },
    {
        "type": "text",
        "name": "batch_size",
        "message": "Batch size:",
        "default": "1000",
        "validate": lambda v: v.isdigit(),
        "filter": lambda v: int(v),
    },
    {"type": "text", "name": "hint", "message": "Insert hint:",},
]

class Sql(BaseEngine):
    @classmethod
    def name(cls):
        return "SQL"

    @classmethod
    def ask(cls):
        connections = db.get_connections_by_category('db')
        source_connection_name = q.select(
            "Select source connection", choices=[c["name"] for c in connections]
        ).ask()

        target_connection_name = q.select(
            "Select source connection", choices=[c["name"] for c in connections]
        ).ask()

        pass