# pylint: disable=broad-except
from contextlib import contextmanager
import logging
import questionary as q
from questionary import Choice
from yaspin import yaspin

from tractor.plugins import registery
from tractor.plugins.solo.base import SoloPlugin
from tractor.util import required

try:
    import cx_Oracle

    ENABLED = True
except ImportError:
    ENABLED = False


logger = logging.getLogger("engine.oracle_dblink")


@contextmanager
def open_connection(config):
    dsn = cx_Oracle.makedsn(
        config["host"],
        config["port"],
        sid=config.get("sid"),
        service_name=config.get("service_name"),
    )
    connection = cx_Oracle.connect(
        user=config["username"], password=config["password"], dsn=dsn
    )
    try:
        yield connection
    finally:
        connection.close()


@yaspin(text="Finding db links...")
def _dblinks(config):
    with open_connection(config) as conn:
        query = "select db_link from all_db_links"
        cursor = conn.cursor()
        cursor.execute(query)
        records = cursor.fetchall()
        cursor.close()
        result = [r[0] for r in records]

    return result


class OracleDbLink(SoloPlugin):
    @classmethod
    def name(cls):
        return "Oracle DB Link"

    @classmethod
    def enabled(cls):
        return ENABLED

    @classmethod
    def ask(cls):
        config = dict()

        config["host"] = q.text("* Host name or ip address", validate=required).ask()
        config["port"] = int(q.text(
            "* Port number",
            default="1521",
            validate=lambda val: val.isdigit() and int(val) in range(1, 65535),
        ).ask())
        service_or_sid = q.select(
            "Service Name or SID",
            choices=[
                Choice(title="Service Name", value="service_name"),
                Choice(title="SID", value="sid"),
            ],
        ).ask()
        config[service_or_sid] = q.text(
            "* Service name" if service_or_sid == "service_name" else "* SID",
            validate=required,
        ).ask()
        config["username"] = q.text("* Username", validate=required).ask()
        config["password"] = q.password("* Password", validate=required).ask()

        config["link"] = q.select("* Select db link", choices=_dblinks(config)).ask()
        config["source"] = q.text("* Source table [schema.table or just table]").ask()
        config["target"] = q.text("Target table [schema.table or just table]").ask()
        if not config["target"]:
            config["target"] = config["source"]

        config["source_columns"] = q.text(
            "* Source columns (comma seperated)", default="*", validate=required
        ).ask()
        config["target_columns"] = q.text("Target columns (comma seperated)").ask()

        return config

    @property
    def query(self):
        return f"""
            create table {self.config['target']}
            as
            select {self.config['source_columns']}
            from {self.config['source']}@{self.config['link']}
        """

    def _drop_target(self, conn):
        query = f"drop table {self.config['target']}"
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            cursor.close()
        except: # pylint: disable=bare-except
            logger.debug("Table does not exists. Continueing...")

    def run(self):
        with open_connection(self.config) as conn:
            self._drop_target(conn)
            cursor = conn.cursor()
            cursor.execute(self.query)
            conn.commit()
            logger.info("✓ Success")

registery.register(OracleDbLink)
