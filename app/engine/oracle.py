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

    mapping_source_questions = BaseEngine.mapping_source_questions + [
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

    mapping_target_questions = BaseEngine.mapping_target_questions + [
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

    def get_select_query(self, props):
        query = f"""
            select {props["columns"]} from {props["table"]}
        """
        if props["filter"] is not None:
            return query + f" where {props['filter']}"

        return query

    def get_fetch_size(self, props):
        DEFAULT_FETCH_SIZE = 1000 # todo move to settings
        return props.get('fetch_size', self.config.get('fetch_size'), DEFAULT_FETCH_SIZE)

    def read(self, props):
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
            cursor.execute(self.get_select_query(props))
            record_count = cursor.rowcount
            if cursor.description is not None:
                columns = [i[0] for i in cursor.description]
                self.channel.put(
                    dict(type="metadata", record_count=record_count, data=columns)
                )

            while True:
                rows = cursor.fetchmany(self.get_fetch_size(props))
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
