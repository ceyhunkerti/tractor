import logging

from tqdm import tqdm
from tractor.plugins import BasePlugin, WiredPlugin, PluginType

logger = logging.getLogger("plugins.output.base")


class OutputPlugin(BasePlugin, WiredPlugin):
    @classmethod
    def type(cls):
        return PluginType.OUTPUT

    def prepare(self):
        if self.config.get('progress', True):
            for message in self.count_channel():
                self.init_progress_bar(message.content)
                break


    def __init__(self, channel, config=None):
        WiredPlugin.__init__(self, channel)
        BasePlugin.__init__(self, config)

        self.pbar = None

    def progress(self, step):
        if self.pbar:
            self.pbar.update(step)

    def close(self):
        if self.pbar:
            self.pbar.close()

        self.task_done()

    def init_progress_bar(self, count):
        self.pbar = tqdm(total=count, desc=self.__class__.__name__)


    def run(self):
        raise NotImplementedError()


class GenericRdbms(OutputPlugin):
    def __init__(self, channel, config=None):
        super().__init__(channel, config=config)
        self.query = None

    def open_connection(self):
        raise NotImplementedError()

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
                buffer += message.content
                if len(buffer) >= self.config.get("batch_size", 1000):
                    cursor.executemany(query, buffer)
                    self.progress(len(buffer))
                    buffer = []

            if len(buffer) > 0:
                cursor.executemany(query, buffer)
                self.progress(len(buffer))

            conn.commit()

        self.close()

