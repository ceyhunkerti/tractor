import os
from tractor.plugins import BasePlugin, WiredPlugin, PluginType
from tractor.plugins.base import MessageType


class InputPlugin(BasePlugin, WiredPlugin):
    @classmethod
    def type(cls):
        return PluginType.INPUT

    def __init__(self, channel, config=None):
        BasePlugin.__init__(self, config)
        WiredPlugin.__init__(self, channel)

    def done(self):
        self.send_message(MessageType.DONE)

    def success(self):
        self.send_message(MessageType.SUCCESS)

    def error(self):
        self.send_message(MessageType.SUCCESS)

    def send_data(self, content):
        self.send_message(MessageType.DATA, content)

    def send_metadata(self, content):
        self.send_message(MessageType.METADATA, content)

    def send_count(self, count):
        self.send_message(MessageType.COUNT, count)

    def run(self):
        raise NotImplementedError()

    def help(self):
        raise NotImplementedError()

class DbInputPlugin(InputPlugin):
    @property
    def query(self):
        table = self.config.get('table', None)
        if table:
            return f"""
                select {self.config['columns']} from {table}
            """
        exit(1)
        query = self.config.get('query', None)

        if query:
            if os.path.isfile(query):
                with open(query) as handle:
                    query = handle.read()

            return query

        raise ValueError("Can't build query from parameters!")

    def count(self, conn):
        cursor = conn.cursor()
        cursor.execute(f"""select count(1) from ({self.query})""")
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def publish_metadata(self, conn, cursor):
        if self.config.get('count', True):
            count = self.count(conn)
            self.send_count(count)

        if self.config.get('metadata', True):
            metadata = {
                'table': self.config.get('table'),
                'columns': []
            }
            for col in cursor.description:
                column = {
                    "name": col[0],
                    "type_code" : col[1].name,
                    "display_size" : col[2],
                    "internal_size" : col[3],
                    "null_ok" : col[4],
                }
                metadata['columns'].append(column)

            self.send_metadata(metadata)
