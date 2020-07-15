from contextlib import contextmanager
import logging
import questionary as q
from questionary import Choice

from app.util import required
from app.plugins.input.base import InputPlugin
from app.plugins import registery

try:
    from hdbcli import dbapi
    dbapi.connect()
    ENABLED = True
except ImportError:
    ENABLED = False


logger = logging.getLogger("plugins.input.hana")


def extract_metadata(cursor):
    metadata = {}
    metadata["columns"] = [{"name": c[0]} for c in cursor.description]
    return metadata


class Hana(InputPlugin):
    @classmethod
    def enabled(cls):
        return ENABLED

    @classmethod
    def ask(cls):
        config = dict()
        config["host"] = q.text("* Host name or ip address", validate=required).ask()
        config["port"] = int(
            q.text(
                "Port number",
                default="1521",
                validate=lambda val: val.isdigit() and int(val) in range(1, 65535),
            ).ask()
        )
        config["username"] = q.text("* Username", validate=required).ask()
        config["password"] = q.password("* Password", validate=required).ask()
        config["source_type"] = q.select(
            "* Source type",
            choices=[
                Choice(title="Table", value="table"),
                Choice(title="Query", value="query"),
                Choice(title="Query file", value="query_file"),
            ],
        ).ask()

        if config["source_type"] == "table":
            config["table"] = q.text(
                '* Table name (eg. "table" or "schema.table")', validate=required
            ).ask()
            config["columns"] = q.text(
                "* Columns seperated with comma", default="*", validate=required
            ).ask()
        elif config["source_type"] == "query":
            config["query"] = q.text("* Select query", validate=required).ask()
        else:
            config["query_file"] = q.text(
                "* Query file (eg. /full/path/to/query_file.sql)", validate=required
            ).ask()

        config["fetch_size"] = int(
            q.text(
                "Fetch size", default="1000", validate=lambda val: val.isdigit()
            ).ask()
        )

        return config

    def _get_connection(self):
        return dbapi.connect(
            address=self.config['host'],
            port=self.config['port'],
            user=self.config['username'],
            password=self.config['password']
        )

    @contextmanager
    def open_connection(self):
        connection = self._get_connection()
        try:
            yield connection
        finally:
            connection.close()

    @property
    def query(self):
        if self.config["source_type"] == "file":
            with open(self.config["query_file"], "r") as file:
                result = file.read()
        elif self.config["source_type"] == "query":
            result = self.config["query"]
        elif self.config["source_type"] == "table":
            result = f"""
                select {self.config['columns']} from {self.config['table']}
            """

        return result

    def count(self, conn):
        cursor = conn.cursor()
        cursor.execute(f"""select count(1) from ({self.query})""")
        row_count = cursor.fetchone()[0]
        cursor.close()
        return row_count

    def run(self):
        error = None
        try:
            with self.open_connection() as conn:
                count = self.count(conn)
                cursor = conn.cursor()
                cursor.execute(self.query)
                self.send_metadata({"count": count, **extract_metadata(cursor)})
                while True:
                    rows = cursor.fetchmany(self.config.get("fetch_size", 1000))
                    if not rows:
                        break
                    self.send_data(rows)
            self.success()
        except Exception as err:  # pylint: disable=broad-except
            logger.error("Read error", exc_info=err)
            self.error()
            error = err

        self.done()
        if error is not None:
            raise error


registery.register(Hana)
