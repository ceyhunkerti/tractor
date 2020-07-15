# import sys
import logging
from queue import Queue
from threading import Thread
import click
import questionary as q
from app import repo
from app.plugins import registery, PluginTypes

logger = logging.getLogger("cli.run")


@click.command("run")
@click.argument("name", default=None, required=False)
def run(name):
    logger.info('Running...')
    if not name:
        name = q.autocomplete(
            "Enter mapping name", choices=repo.get_mapping_names()
        ).ask()

    mapping = repo.get_mapping(name)

    runners = dict()
    channel = Queue()
    for _type in ["input", "output", "solo"]:
        runners[_type] = [
            registery.get_item(PluginTypes(_type), config['plugin'])(config)
            if _type == "solo"
            else registery.get_item(PluginTypes(_type), config['plugin'])(
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
