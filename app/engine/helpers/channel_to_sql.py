import sys
import logging
import questionary as q

from app import repo
from app.engine.base import BaseEngine
from app.store import registery as store_registery

logger = logging.getLogger("engine.sql")

class ChannelToSql(BaseEngine):
    @classmethod
    def name(cls):
        return "Sql to Channel"

    @classmethod
    def ask(cls):
        connections = repo.get_connections_by_category("db")
        if len(connections) == 0:
            logger.info(
                """
                There is no "db" category connection!
                Create connections first.
                You can run the following command to create connection.

                tractor config add connection
            """
            )
            sys.exit(1)

        props = dict()

        props['connection'] = q.select(
            "Select target connection", choices=[c["name"] for c in connections]
        ).ask()

        props['table'] = q.text("Target table [schema.table or just table]:").ask()

        props['columns'] = q.text("Target columns seperated by comma or * for all:").ask()

        props['batch_size'] = q.text(
            "Batch size:",
            default=1000,
            validate=lambda v: v.isdigit(),
            filter=int,
        ).ask()

        return props


    @property
    def query(self):
        return f"""
            insert into {self.config['table']}
            values ( {','.join([ ':' + c for c in self.config['columns']])} )
        """

    def truncate_table(self, conn):
        cursor = conn.cursor()
        query = f"truncate table {self.config['table']}"
        logger.debug(query)
        cursor.execute(query)
        conn.commit()
        cursor.close()


    def run(self, channel):
        connection = repo.get_connection_by_name_or_slug(self.config["connection"])
        store_class = store_registery.get_item_by_key(connection["store"])
        store = store_class(connection["props"])
        with store.open_connection() as conn:
            self.truncate_table(conn)
            cursor = conn.cursor()
            rowcount = 0
            data = []
            while True:
                message = channel.get()
                if message['type'] == 'data':
                    if len(message['content']) < self.config.get('batch_size', 1000):
                        data += message['content']
                    else:
                        cursor.executemany(self.query, message['content'])
                        rowcount += len(data)
                        data = []
                        conn.commit()
                elif message['type'] == 'done':
                    channel.task_done()
                    if len(data) > 0:
                        cursor.executemany(self.query, message['content'])
                        rowcount += len(data)
                        data = []
                        conn.commit()
                    break

