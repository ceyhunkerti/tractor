import logging
import questionary as q
from questionary import Choice

from app import db
from .base import BaseEngine
from app.engine import register

logger = logging.getLogger("engine.sql")

class Sql(BaseEngine):
    @classmethod
    def name(cls):
        return "SQL"

    @classmethod
    def ask(cls):
        connections = db.get_connections_by_category("db")
        if len(connections) == 0:
            logger.info(
                """
                There is no "db" category connection!
                Create connections first.
                You can run the following command to create connection.

                dumper config add connection
            """
            )
            exit(1)

        source_connection_name = q.select(
            "Select source connection", choices=[c["name"] for c in connections]
        ).ask()

        target_connection_name = q.select(
            "Select source connection", choices=[c["name"] for c in connections]
        ).ask()

        source_table = q.text("Source table [schema.table or just table]:")

        target_table = q.text("Target table [schema.table or just table]:")
        source_columns = q.text("Source columns seperated by comma or * for all:")

        use_source_columns = q.select(
            "User source columns as target:",
            choices=[Choice(title="Yes", value=True), Choice(title="No", value=False),],
            default=True,
        )
        if not use_source_columns:
            target_columns = q.text("Target columns seperated by comma or * for all:")
        else:
            target_columns = source_columns

        select_hint = q.text("Select query hint:")
        insert_hint = q.text("Insert query hint:")
        fetch_size = q.text(
            "Fetch size:",
            default="1000",
            validate=lambda v: v.isdigit(),
            filter=lambda v: int(v),
        )
        batch_size = q.text(
            "Batch size:",
            default="1000",
            validate=lambda v: v.isdigit(),
            filter=lambda v: int(v),
        )

        create_target_table = q.select(
            "Create target table:",
            choices=[
                Choice(title="Yes", value="yes"),
                Choice(title="No", value="no"),
                Choice(title="Yes if not exists", value="yes_if_not_exists"),
            ],
            default="yes_if_not_exists",
        )


        return dict(
            source=dict(
                connection=source_connection_name,
                table=source_table,
                hint=select_hint,
                source_columns=source_columns,
                fetch_size=fetch_size,
            ),
            target=dict(
                connection=target_connection_name,
                table=target_table,
                hint=insert_hint,
                batch_size=batch_size,
                target_columns=target_columns,
                create=create_target_table
            ),
        )

register(Sql)