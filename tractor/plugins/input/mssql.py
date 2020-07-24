from contextlib import contextmanager
import logging

from tractor.plugins.input.base import DbInputPlugin
from tractor.plugins import registery

try:
    import pymssql

    ENABLED = True
except ImportError:
    ENABLED = False


logger = logging.getLogger("plugins.input.oracle")


class MsSql(DbInputPlugin):
    """
        host:[required]     = host name or ip address
        port:[1433]         = Port number
        username            = Connection username
        password            = Connection password or environment variable $PASSWORD
        database            = Database name
        table:[*]           = Table name schema.table_name or table_name
        columns             = [{name: column_name, type: column_type}, ...]
        query:[*]           = Query file or query string
        batch_size          = Batch insert size
        metadata:[True]     = Send metadata to ouput plugin
        count:[True]        = Send count to ouput plugin

        * either query or table must be given
    """

    @classmethod
    def enabled(cls):
        return ENABLED

    def count(self, conn):
        cursor = conn.cursor()
        cursor.execute(f"""select count_big(1) from ({self.query}) q""")
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    @contextmanager
    def open_connection(self):
        connection = pymssql.connect(
            server=self.config['host'],
            user=self.config['username'],
            password=self.config['password'],
            database=self.config['database'],
            port=self.config.get('port', 1433)
        )
        try:
            yield connection
        finally:
            connection.close()


    def run(self):
        error = None
        try:
            with self.open_connection() as conn:
                self._send_count(conn)
                cursor = conn.cursor()
                cursor.execute(self.query)
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


registery.register(MsSql)
