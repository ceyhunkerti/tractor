import logging
import click
import questionary as q
from questionary import Choice, Separator
from app.plugins import registery
from app.plugins import PluginTypes

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
    config["name"] = name

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
            config[plugin_type] = []

        plugin = q.select(
            "Select plugin",
            choices=[
                Choice(title=plugin.name(), value=plugin)
                for plugin in registery.items(plugin_type)
            ],
        ).ask()

        options = plugin.ask()
        config[plugin_type].append({"plugin": plugin.name(), **options})

    print(config)
