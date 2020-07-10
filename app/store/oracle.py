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


class Oracle(BaseStore):

    categories = BaseStore.categories + ["db"]

    questions = BaseStore.questions + _questions

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


registery.register(Oracle)
