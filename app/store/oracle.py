import questionary
from .base import BaseStore
from . import register
from app.util import required

try:
    import cx_Oracle

    enabled = True
except ImportError:
    enabled = False


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
        "filter": lambda val: int(val),
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
        return enabled

    @classmethod
    def type(cls):
        return "oracle"

    @classmethod
    def _convert_number(cls, value):
        try:
            return int(value)
        except:
            return value

    @classmethod
    def output_handler(cls, cursor, name, default_type, length, precision, scale):
        if default_type in (cx_Oracle.CLOB, cx_Oracle.LOB):
            return cursor.var(cx_Oracle.LONG_STRING, 80000, cursor.arraysize)

        if default_type in (cx_Oracle.STRING, cx_Oracle.FIXED_CHAR):
            return cursor.var(str, length, cursor.arraysize)

        if default_type == cx_Oracle.NUMBER:
            if scale <= 0:
                return cursor.var(
                    cx_Oracle.STRING,
                    255,
                    outconverter=Oracle._convert_number,
                    arraysize=cursor.arraysize,
                )

    def __init__(self, config):
        super(Oracle, self).__init__(config)


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
        connection.outputtypehandler = Oracle.output_handler
        return connection



register(Oracle)
