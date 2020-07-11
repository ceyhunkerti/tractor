# pylint: disable=broad-except

from contextlib import contextmanager
import sys
import questionary as q
from questionary import Choice
from yaspin import yaspin
from cx_Oracle import DatabaseError

from app import repo, logging
from app.engine import registery
from app.store import registery as store_registery
from .base import BaseEngine


logger = logging.getLogger("engine.oracle_dblink")


def get_schema_table(connection_name, table, upcase=True):
    if "." in table:
        owner, table_name = table.split(".")
    else:
        connection = repo.get_connection(connection_name)
        owner = connection["username"]
        table_name = table

    if '"' not in table_name and upcase:
        table_name = table_name.upper()
    if '"' not in owner and upcase:
        owner = owner.upper()

    return owner, table_name


@contextmanager
def open_connection(name):
    # pylint: disable=invalid-name
    connection = repo.get_connection(name)
    StoreClass = store_registery.get_item_by_key(connection.get("store"))
    store = StoreClass(connection["props"])
    conn = store.get_connection()
    try:
        yield conn
    finally:
        conn.close()


@yaspin(text="Finding db links...")
def get_dblinks(connection_name):
    with open_connection(connection_name) as conn:
        query = "select db_link from all_links"
        cursor = conn.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        cursor.close()
        result = [r[0] for r in records]

    return result


@yaspin(text="Fetching column list...")
def get_columns_with_link(connection_name, db_link, table):
    owner, table_name = get_schema_table(connection_name, table)

    with open_connection(connection_name) as conn:
        query = f"""
            select
                column_name, data_type
            from
                all_tab_cols@{db_link}
            where
                owner = '{owner.replace('"', '')}' and table_name = '{table_name.replace('"', '')}'
        """
        logger.debug(query)
        cursor = conn.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        cursor.close()
        result = [{"column_name": r[0], "data_type": r[1]} for r in records]

    return result


class OracleDbLink(BaseEngine):
    @classmethod
    def name(cls):
        return "Oracle DB Link"

    @classmethod
    def type(cls):
        return "oracle_dblink"

    @classmethod
    def ask(cls):
        props = dict()
        connections = repo.get_connections_by_store("oracle")
        if len(connections) == 0:
            logger.info(
                """
                Create connections first.
                You can run the following command to create connection.

                tractor config add connection
            """
            )
            sys.exit(1)

        props["connection"] = q.select(
            "Select target connection", choices=[c["name"] for c in connections]
        ).ask()
        links = get_dblinks(props["connection"])

        props["link"] = q.select("Select database link", choices=links).ask()

        props["source_table"] = q.text(
            "Source table [schema.table or just table]"
        ).ask()
        props["target_table"] = q.text(
            "Target table [schema.table or just table]"
        ).ask()

        column_selection = q.select(
            "Columns", choices=["Select from list", "Enter as text"]
        ).ask()

        if column_selection == "Enter as text":
            props["columns"] = q.text(
                "Enter comma separated columns or * for all:", default="*"
            )
        else:
            column_list = get_columns_with_link(
                props["connection"], props["link"], props["source_table"]
            )
            choices = [
                Choice(
                    title=f"{c['column_name']} - {c['data_type']}",
                    value=c["column_name"],
                )
                for c in column_list
            ]
            props["columns"] = q.checkbox("Select columns", choices=choices).ask()

        props["source_hint"] = q.text("Select hint (eg. parallel 16):").ask()

        # or "compress" if you drop/create the target table
        props["target_hint"] = q.text("Target hint (eg. append, nologging):").ask()

        props["create_table"] = q.select(
            "Create target table:",
            choices=[
                Choice(title="Yes", value=True),
                Choice(title="No", value=False),
                Choice(title="Yes if not exists", value="if_not_exists"),
            ],
            default=True,
        ).ask()

        return props

    @yaspin(text="Running...")
    def run(self):
        connection = repo.get_connection_by_name_or_slug(self.config["connection"])
        store_class = store_registery.get_item_by_key(connection["store"])
        oracle = store_class(connection["props"])
        query = f"""
            select /*+ ${self.config['source_hint']} */
                {",".join(self.config['columns'])}
            from {self.config['source_table']}@{self.config['link']}
        """

        create_table = self.config["create_table"]

        if create_table == "if_not_exists" or create_table:
            query = f"""
                create table {self.config['target_table']}
                ${self.config['target_hint']}
                as
                {query}
            """
        else:
            query = f"""
                insert /*+ ${self.config['target_hint']} */
                    into {self.config['target_table']}
                {query}
            """

        with oracle.open_connection() as conn:
            if create_table == "if_not_exists" or create_table:
                logger.debug("Dropping target table...")
                drop_query = f"drop table {self.config['target_table']}"
                logger.debug(drop_query)
                cursor = conn.cursor()
                try:
                    cursor.execute(drop_query)
                except DatabaseError:
                    logger.debug("Table does not exists. Continueing...")
            else:
                truncate_query = f"truncate table {self.config['target_table']}"
                logger.debug("Truncating target table...")
                logger.debug(truncate_query)
                cursor = conn.cursor()
                cursor.execute(truncate_query)
            cursor.close()
            logger.debug(query)
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()

        logger.info("✓ Success")



registery.register(OracleDbLink)
