import sys
import logging
import questionary as q

from app import repo
from app.engine.base import BaseEngine
from app.store import registery as store_registery

logger = logging.getLogger("engine.sql")

class SqlToChannel(BaseEngine):
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
            "Select source connection", choices=[c["name"] for c in connections]
        ).ask()


        props['table'] = q.text("Source table [schema.table or just table]:").ask()

        props['columns'] = q.text("Source columns seperated by comma or * for all:").ask()

        props['hint'] = q.text("Select query hint:").ask()

        props['fetch_size'] = q.text(
            "Fetch size:",
            default="1000",
            validate=lambda v: v.isdigit(),
            filter=int,
        ).ask()

        return props


    @property
    def query(self):
        return f"""
            select {self.config['hint']}
                {",".join(self.config['columns'])}
            from
                {self.config['table']}
        """

    def run(self, channel):
        try:
            connection = repo.get_connection_by_name_or_slug(self.config["connection"])
            store_class = store_registery.get_item_by_key(connection["store"])
            store = store_class(connection["props"])
            with store.open_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(self.query)
                while True:
                    rows = cursor.fetchmany(self.config.get('fetch_size', 1000))
                    if not rows:
                        break
                    channel.add(dict(type="data", content=rows))
            channel.add(dict(type="done", content="success"))
        except Exception as err:
            logger.error("Read error")
            channel.add(dict(type="done", content="error"))
            raise err

        channel.add(dict(type="done", content="done"))
