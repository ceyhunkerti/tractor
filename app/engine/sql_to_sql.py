from queue import Queue
from threading import Thread
import sys
import logging
import questionary as q
from questionary import Choice

from app.engine import registery
from app import repo
from .base import BaseEngine
from .helpers import SqlToChannel, ChannelToSql

logger = logging.getLogger("engine.sql")

class SqlToSql(BaseEngine):
    @classmethod
    def name(cls):
        return "Sql to Sql"

    @classmethod
    def ask(cls):
        props = dict()
        props['source'] = SqlToChannel.ask()
        props['target'] = ChannelToSql.ask()

        return props

    def run(self):
        source = SqlToChannel(self.config['source'])
        target = ChannelToSql(self.config['target'])

        channel = Queue()
        reader = Thread(target=source.run, args=(channel, ))
        writer = Thread(target=target.run, args=(channel, ))
        reader.start()
        writer.start()

        for thread in [reader, writer]:
            thread.join()


registery.register(SqlToSql)
