import os
from contextlib import contextmanager
import logging

from tractor.plugins.input.base import DbInputPlugin
from tractor.plugins import registery

try:
    import cx_Oracle

    ENABLED = True
except ImportError:
    ENABLED = False


logger = logging.getLogger("plugins.input.oracle")


class Oracle(DbInputPlugin):
    @classmethod
    def enabled(cls):
        return ENABLED

    def help(self):
        print("""
            host:[required]     = Path to input file
            port:[1521]         = Field delimiter
            username            = Connection username
            password            = Connection password or environment variable $PASSWORD
            sid:[*]             = Record delimiter
            serivce_name:[*]    = Count records and send to output plugin
            table:[*]           = Table name schema.table_name or table_name
            columns             = [{name: column_name, type: column_type}, ...]
            query:[*]           = Query file or query string
            batch_size          = Batch insert size
            metadata:[True]     = Send metadata to ouput plugin
            count:[True]        = Send count to ouput plugin

            * either service_name or sid must be given
            * either query or table must be given
        """)

    @contextmanager
    def open_connection(self):
        dsn = cx_Oracle.makedsn(
            self.config["host"],
            self.config.get("port", 1521),
            sid=self.config.get("sid"),
            service_name=self.config.get("service_name"),
        )
        password = os.environ.get(self.config["password"][1:], self.config["password"])
        connection = cx_Oracle.connect(
            user=self.config["username"], password=password, dsn=dsn
        )
        try:
            yield connection
        finally:
            connection.close()


    def run(self):
        error = None
        try:
            with self.open_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(self.query)
                self.publish_metadata(conn, cursor)

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


registery.register(Oracle)
