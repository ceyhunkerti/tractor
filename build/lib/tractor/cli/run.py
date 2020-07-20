import os
import logging
from queue import Queue
from threading import Thread
import click
import questionary as q
from tractor import repo
from tractor.plugins import registery, PluginType

logger = logging.getLogger("cli.run")


def get_mapping(mapping):
    if not os.path.isfile(mapping):
        return repo.get_mapping(mapping)

    with open(mapping) as handler:
        content = handler.read()

    return content

@click.command("run")
@click.argument("name", default=None, required=False)
def run(name):
    logger.info('Running...')
    if not name:
        name = q.autocomplete(
            "Enter mapping name", choices=repo.get_mapping_names()
        ).ask()

    mapping = get_mapping(name)

    runners = dict()
    channel = Queue()
    for _type in ["input", "output", "solo"]:
        runners[_type] = [
            registery.get_item(PluginType(_type), config['plugin'])(config)
            if _type == "solo"
            else registery.get_item(PluginType(_type), config['plugin'])(
                channel, config
            )
            for config in mapping.get(_type, [])
        ]

    threads = []
    for _type in ["solo", "output", "input"]:
        for runner in runners[_type]:
            threads.append(Thread(target=runner.run))


    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    logger.info('Done')
