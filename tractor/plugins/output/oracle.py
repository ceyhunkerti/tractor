from contextlib import contextmanager
import logging

from tractor.plugins.output.base import GenericRdbms
from tractor.plugins import registery


try:
    import cx_Oracle

    ENABLED = True
except ImportError:
    ENABLED = False


logger = logging.getLogger("plugins.output.oracle")


class Oracle(GenericRdbms):
    """
        host:[required]     = Hostname or ip address
        port:[1521]         = Port number
        username            = Connection username
        password            = Connection password or environment variable $PASSWORD
        sid:[*]             = Record delimiter
        serivce_name:[*]    = Count records and send to output plugin
        table:[*]           = Table name schema.table_name or table_name
        columns             = [{name: column_name, type: column_type}, ...] or [column1, column2,]
        query:[*]           = Query file or query string
        batch_size          = Batch insert size
        truncate[True/Flase]= Truncate target table
        create[True/Flase]  = Create target table

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

registery.register(Oracle)
