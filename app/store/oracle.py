from contextlib import contextmanager
from app.util import required
from .base import BaseStore
from . import registery

try:
    import cx_Oracle

    ENABLED = True
except ImportError:
    ENABLED = False


_questions = [
    {
        "type": "text",
        "name": "host",
        "message": "Host name or ip address:",
        "validate": required,
    },
    {
        "type": "text",
        "default": "1521",
        "name": "port",
        "message": "Port number",
        "validate": lambda val: val.isdigit() and int(val) in range(1, 65535),
        "filter": int,
    },
    {
        "type": "select",
        "name": "service_or_sid",
        "message": "Will you provide 'Serivce Name' or 'SID'",
        "choices": ["Service Name", "SID"],
    },
    {
        "when": lambda x: x["service_or_sid"] == "Service Name",
        "type": "text",
        "name": "service_name",
        "message": "Enter service name:",
        "validate": required,
    },
    {
        "when": lambda x: x["service_or_sid"] == "SID",
        "type": "text",
        "name": "sid",
        "message": "Enter sid:",
        "validate": required,
    },
    {"type": "text", "name": "username", "message": "Username:", "validate": required},
    {
        "type": "password",
        "name": "password",
        "message": "Password:",
        "validate": required,
    },
]

_settings = {
    "scan": {
        "db_links": ['LNK_ODIVM2'],
        "object_types": ['VIEW', 'TABLE'],
        "schemas": ['TRUCK']
    }
}


class Oracle(BaseStore):

    categories = BaseStore.categories + ["db"]

    questions = BaseStore.questions + _questions

    settings = {**BaseStore.settings, **_settings}

    @classmethod
    def name(cls):
        return "Oracle"

    @classmethod
    def enabled(cls):
        return ENABLED

    @classmethod
    def type(cls):
        return "oracle"

    @classmethod
    def _convert_number(cls, value):
        try:
            return int(value)
        except:  # pylint: disable=bare-except
            return value

    def get_connection(self):
        dsn = cx_Oracle.makedsn(
            self.config["host"],
            self.config["port"],
            sid=self.config.get("sid", None),
            service_name=self.config.get("service_name", None),
        )
        connection = cx_Oracle.connect(
            user=self.config["username"], password=self.config["password"], dsn=dsn
        )
        return connection

    @contextmanager
    def open_connection(self):
        connection = self.get_connection()
        try:
            yield connection
        finally:
            connection.close()


    def self_scan(self, object_types):
        with self.open_connection() as conn:
            query = f"""
                select object_name, object_type
                from all_objects
                where object_type in ('TABLE', 'VIEW')
                and owner not in ({",".join(["'" + t + "'" for t in object_types])})
            """
            cursor = conn.cursor()
            cursor.execute(query)

            result = [{'name': f"{r[0]} - {r[1]}", "value": r[0]} for r in cursor]

        return result

    def scan(self, settings=None):
        settings = self.settings if settings is None else settings
        if not settings.scan:
            return None

        index = dict(items=self.self_scan(settings))

        return index


registery.register(Oracle)
