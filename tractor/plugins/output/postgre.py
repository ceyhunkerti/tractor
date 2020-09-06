from contextlib import contextmanager
import logging

from tractor.plugins.output.base import GenericRdbms
from tractor.plugins import registery

try:
    import psycopg2

    ENABLED = True
except ImportError:
    ENABLED = False


logger = logging.getLogger("plugins.output.prostgre")


class Oracle(GenericRdbms):
    """
        host:[required]     = Hostname or ip address
        port:[5432]         = Port number
        username            = Connection username
        password            = Connection password or environment variable $PASSWORD
        database            = Database to connect
        table               = name of the target table
        columns             = [{name: column_name, type: column_type}, ...] or [column1, column2,]
        * either service_name or sid must be given
        * either query or table must be given
    """

    @classmethod
    def enabled(cls):
        return ENABLED

    def __init__(self, channel, config=None):
        super().__init__(channel, config=config)

    @contextmanager
    def open_connection(self):
        connection = psycopg2.connect(
            user=self.config['username'],
            password=self.config['password'],
            host=self.config['host'],
            port=self.config.get('port', 5432),
            database=self.config['database'],
        )
        try:
            yield connection
        finally:
            connection.close()


registery.register(Oracle)
