from .registery import Registery
from .base import PluginType, BasePlugin, WiredPlugin


registery = Registery()

def import_plugins(plugins):
    for plugin in plugins:
        __import__(plugin)
