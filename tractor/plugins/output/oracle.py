from contextlib import contextmanager
import logging

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
    def enabled(cls):
        return ENABLED

    def __init__(self, channel, config=None):
        super().__init__(channel, config=config)
        self.query = None

    @contextmanager
    def open_connection(self):
        dsn = cx_Oracle.makedsn(
            self.config["host"],
            self.config.get("port", 1521),
            sid=self.config.get("sid"),
            service_name=self.config.get("service_name"),
        )
        connection = cx_Oracle.connect(
            user=self.config["username"], password=self.config["password"], dsn=dsn
        )
        try:
            yield connection
        finally:
            connection.close()

    def build_query(self, conn):
        table = self.config['table']
        columns = self.config.get('columns')

        if not isinstance(columns, list):
            cursor = conn.cursor()
            cursor.execute(f"select * from {table}")
            columns = [c[0] for c in cursor.description]
            cursor.close()

        _columns = [c['name'] if isinstance(c, dict) else c for c in columns]
        return f"""
            insert into {table} ({",".join(_columns)}) values (
                {",".join([":" + c for c in _columns])}
            )
        """


    def _drop_table(self, conn):
        try:
            table = self.config.get('table')
            query = f"drop table {table}"
            logger.debug(query)
            cursor = conn.cursor()
            cursor.execute(query)
            cursor.close()
        except: # pylint: disable=bare-except
            logger.debug("Failed to drop table %s", table)

    def _create_table(self, conn):
        self._drop_table(conn)

        table = self.config.get('table')
        columns = self.config.get('columns')

        if table and isinstance(columns, list):
            query = f"""
                create table {table} (
                    {
                        ",".join([c['name'] + " " + c['type']  for c in columns])
                    }
                )
            """
            logger.debug(query)
            cursor = conn.cursor()
            cursor.execute(query)
            cursor.close()
            logger.info("%s - table created", table)


    def _truncate_table(self, conn):
        cursor = conn.cursor()
        query = f"truncate table {self.config['table']}"
        logger.debug(query)
        cursor.execute(query)
        conn.commit()
        cursor.close()

    def _check_table(self, conn):
        if self.config.get('truncate', False):
            try:
                self._truncate_table(conn)
            except: # pylint: disable=bare-except
                logger.error("Failed to truncate table")
        elif self.config.get('create', False):
            try:
                self._create_table(conn)
            except: # pylint: disable=bare-except
                logger.error("Failed to create table")

    def _prepare(self, conn):
        self._check_table(conn)
        self.query = self.build_query(conn)
        if self.config.get('progress', True):
            for message in self.count_channel():
                self.init_progress_bar(message.content)
                break

    def run(self):
        with self.open_connection() as conn:
            self._prepare(conn)

            buffer = []
            query = self.build_query(conn)
            cursor = conn.cursor()
            for message in self.data_channel():
                if len(buffer) <= self.config.get("batch_size", 1000):
                    buffer += message.content
                else:
                    cursor.executemany(query, buffer)
                    self.progress(len(buffer))
                    buffer = []

            if len(buffer) > 0:
                cursor.executemany(query, buffer)
                self.progress(len(buffer))

            conn.commit()

        self.close()

registery.register(Oracle)
