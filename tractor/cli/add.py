import sys
import logging
import click
import questionary as q
from questionary import Choice, Separator
from tractor.plugins import PluginTypes, registery
from tractor import repo

logger = logging.getLogger("config.add")


@click.group()
def add():
    """Add config commands"""


@add.command(name="mapping", help="Add new mapping")
@click.argument("name", default=None, required=False)
def mapping(name):
    config = dict()
    if name is None:
        name = q.text("Enter mapping name:").ask()

    if repo.get_mapping(name):
        logger.error('Mapping with %s already exists', name)
        sys.exit(1)

    print("---- Add Plugins ----")
    while True:
        plugin_type = q.select(
            "Select plugin type",
            choices=[
                Choice(title="Input", value=PluginTypes.INPUT),
                Choice(title="Output", value=PluginTypes.OUTPUT),
                Choice(title="Solo", value=PluginTypes.SOLO),
                Separator(),
                Choice(title="Exit", value=False),
            ],
        ).ask()

        if not plugin_type:
            break

        if not config.get(plugin_type):
            config[plugin_type.value] = []

        plugin = q.select(
            "Select plugin",
            choices=[
                Choice(title=plugin.name(), value=plugin)
                for plugin in registery.items(plugin_type)
            ],
        ).ask()

        options = plugin.ask()
        config[plugin_type.value].append({"plugin": plugin.name(), **options})

    repo.add_mapping(name, config)
