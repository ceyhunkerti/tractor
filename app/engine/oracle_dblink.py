# pylint: disable=broad-except

import sys
import logging
import questionary as q
from questionary import Choice
from yaspin import yaspin

from app import repo
from app.engine import registery
from app.store import get_store
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


def get_connection(connection_name):
    connection = repo.get_connection(connection_name)
    store = get_store(connection["store"])
    return store.get_connection()


@yaspin(text="Finding db links...")
def get_dblinks(connection_name):
    result = None
    error = None
    try:
        conn = get_connection(connection_name)
        query = "select db_link from all_db_links"
        cursor = conn.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        cursor.close()
        result = [r[1] for r in records]
    except Exception as err:
        logger.error("Failed to get db link list")
        error = err
    finally:
        conn.close()

    return result, error


@yaspin(text="Fetching column list...")
def get_columns(connection_name, table):
    result = None
    error = None
    try:
        owner, table_name = get_schema_table(connection_name, table)
        conn = get_connection(connection_name)
        query = f"""
            select
                column_name, data_type
            form
                all_tab_cols
            where
                owner = '{owner.replace('"', '')}' and table_name = {table_name.replace('"', '')}
        """
        cursor = conn.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        cursor.close()
        result = [{"column_name": r[0], "data_type": r[1]} for r in records]
    except Exception as err:
        error = err
    finally:
        conn.close()

    return result, error


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
        connections = repo.get_connections_by_type("oracle")
        if len(connections) == 0:
            logger.info(
                """
                Create connections first.
                You can run the following command to create connection.

                tractor config add connection
            """
            )
            sys.exit(1)

        props['connection'] = q.select(
            "Select target connection", choices=[c["name"] for c in connections]
        ).ask()
        db_links, error = get_dblinks(props['connection'])

        if error is not None:
            logger.error(error)
            sys.exit(1)

        props['db_link'] = q.select("Select database link", choices=db_links).ask()

        source_table = q.text("Source table [schema.table or just table]").ask()
        props['target_table'] = q.text("Target table [schema.table or just table]").ask()

        column_selection = q.select(
            "Columns", choices=["Select from list", "Enter as text"]
        )

        if column_selection == "Enter as text":
            props['columns'] = q.text("Enter comma separated columns or * for all:", default="*")
        else:
            column_list = get_columns(props['connection'], source_table)
            choices = [
                Choice(
                    title=f"{c['column_name']} - {c['data_type']}",
                    value=c["column_name"],
                )
                for c in column_list
            ]
            props['columns'] = q.select("Select columns", choices=choices).ask()

        props['source_hint'] = q.text("Select hint (eg. parallel 16):").ask()

        # or "compress" if you drop/create the target table
        props['target_hint'] = q.text("Target hint (eg. append, nologging):").ask()

        props['drop_create'] = q.select(
            "Drop create:",
            choices=[Choice(title="Yes", value=True), Choice(title="No", value=False),],
            default=True,
        )

        return props


registery.register(OracleDbLink)
