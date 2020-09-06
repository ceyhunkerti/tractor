# import sys
import logging
from queue import Queue
from threading import Thread
import click
from tractor import repo
from tractor.plugins import registery, PluginType

logger = logging.getLogger("cli.run")


@click.command("run")
@click.argument("name", required=True)
def run(name):
    logger.info('Running...')

    mapping = repo.get_mapping(name)

    incls = registery.get_item(PluginType.INPUT, mapping['input']['plugin'])
    outcls = registery.get_item(PluginType.OUTPUT, mapping['output']['plugin'])

    # print(incls, outcls)

    channel = Queue()
    _in = incls(channel, mapping['input'])
    _out = outcls(channel, mapping['output'])

    threads = []
    threads.append(Thread(target=_in.run))
    threads.append(Thread(target=_out.run))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    logger.info('Done')
