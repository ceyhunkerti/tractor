import questionary
from .base import BaseStore
from . import register

try:
    import cx_Oracle

    enabled = True
except ImportError:
    enabled = False

_connection_questions = [
    {"type": "text", "name": "host", "message": "Host name or ip address:",},
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
    },
    {
        "when": lambda x: x["service_or_sid"] == "SID",
        "type": "text",
        "name": "sid",
        "message": "Enter sid:",
    },
    {"type": "text", "name": "username", "message": "Username:",},
    {"type": "text", "name": "password", "message": "Password:",},
]

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


class Oracle(BaseStore):
    connection_questions = BaseStore.connection_questions + _connection_questions
    reader_questions = BaseStore.reader_questions + _reader_questions
    writer_questions = BaseStore.writer_questions + _writer_questions

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

    def __init__(self, channel, config):
        super(Oracle, self).__init__(channel, config)

    def get_select_query(self, rc):
        query = f"""
            select {rc["columns"]} from {rc["table"]}
        """
        if rc["filter"] is not None:
            return query + f" where {rc['filter']}"

        return query

    def get_fetch_size(self, rc):
        DEFAULT_FETCH_SIZE = 1000  # todo move to settings
        return rc.get(
            "fetch_size", self.config.get("fetch_size"), DEFAULT_FETCH_SIZE
        )

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

    def read(self, rc):
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
        cursor = connection.cursor()

        try:
            cursor.execute(self.get_select_query(rc))
            record_count = cursor.rowcount
            if cursor.description is not None:
                columns = [i[0] for i in cursor.description]
                self.channel.put(
                    dict(type="metadata", record_count=record_count, data=columns)
                )

            while True:
                rows = cursor.fetchmany(self.get_fetch_size(rc))
                if not rows:
                    break
                self.channel.put(dict(type="metadata", data=rows))

        except cx_Oracle.DatabaseError as err:
            error = "Query failed. {}.".format(str(err))
        except KeyboardInterrupt:
            connection.cancel()
            raise
        finally:
            connection.close()


register(Oracle)
