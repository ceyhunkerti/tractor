from contextlib import contextmanager
import logging
import questionary as q
from questionary import Choice


from tractor.settings.helpers import parse_boolean
from tractor.util import required
from tractor.plugins.output.base import OutputPlugin
from tractor.plugins import registery

try:
    import cx_Oracle

    ENABLED = True
except ImportError:
    ENABLED = False


logger = logging.getLogger("plugins.output.oracle")


class Oracle(OutputPlugin):
    @classmethod
    def name(cls):
        return "Oracle"

    @classmethod
    def enabled(cls):
        return ENABLED

    @classmethod
    def ask(cls):
        config = dict()
        config["host"] = q.text("* Host name or ip address", validate=required).ask()
        config["port"] = int(q.text(
            "* Port number",
            default="1521",
            validate=lambda val: val.isdigit() and int(val) in range(1, 65535),
        ).ask())
        service_or_sid = q.select(
            "* Service Name or SID",
            choices=[
                Choice(title="Service Name", value="service_name"),
                Choice(title="SID", value="sid"),
            ],
        ).ask()
        config[service_or_sid] = q.text(
            "* Service name" if service_or_sid == "service_name" else "SID",
            validate=required,
        ).ask()
        config["username"] = q.text("* Username", validate=required).ask()
        config["password"] = q.password("* Password", validate=required).ask()

        config["table"] = q.text('* Table name (eg. "table" or "schema.table")').ask()
        config["columns"] = q.text("Columns (optional, comma seperated)").ask()

        config["batch_size"] = int(q.text(
            "Batch size", default="1000", validate=lambda val: val.isdigit()
        ).ask())

        return config

    def _get_connection(self):
        dsn = cx_Oracle.makedsn(
            self.config["host"],
            self.config["port"],
            sid=self.config.get("sid"),
            service_name=self.config.get("service_name"),
        )
        connection = cx_Oracle.connect(
            user=self.config["username"], password=self.config["password"], dsn=dsn
        )
        return connection

    @contextmanager
    def open_connection(self):
        connection = self._get_connection()
        try:
            yield connection
        finally:
            connection.close()

    @property
    def query(self):
        bindings = ",".join(
            [":" + str(i) for i in range(1, self.config["column_count"] + 1)]
        )
        if len(self.config.get("columns", [])) > 0:
            return f"""
                insert into {self.config['table']} {"("+ self.config['columns'] + ")"}
                values ( {bindings} )
            """

        return f"""
            insert into {self.config['table']}
            values ( {bindings} )
        """

    def _truncate_table(self, conn):
        if not parse_boolean(self.config.get("truncate", "yes")):
            return
        cursor = conn.cursor()
        query = f"truncate table {self.config['table']}"
        logger.debug(query)
        cursor.execute(query)
        conn.commit()
        cursor.close()

    def run(self):
        with self.open_connection() as conn:
            self._truncate_table(conn)
            cursor = conn.cursor()
            rowcount = 0
            buffer = []
            while True:
                message = self.channel.get()
                if message["type"] == self.MessageTypes.METADATA:
                    self.set_metadata(message["content"])
                elif message["type"] == self.MessageTypes.DATA:
                    self.progress(len(message["content"]))
                    if len(message["content"]) < self.config.get("batch_size", 1000):
                        buffer += message["content"]
                    else:
                        cursor.executemany(self.query, message["content"])
                        rowcount += len(buffer)
                        buffer = []
                        conn.commit()
                elif message["type"] == self.MessageTypes.STATUS:
                    if message["content"] == self.Status.SUCCESS:
                        if len(buffer) > 0:
                            cursor.executemany(self.query, buffer)
                            rowcount += len(buffer)
                            buffer = []
                            conn.commit()
                    else:
                        logger.error("Received error status from channel")

                    self.close()
                    break



registery.register(Oracle)
