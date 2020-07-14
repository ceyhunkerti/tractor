from app.plugins import BasePlugin, WiredPlugin, PluginTypes

class OutputPlugin(BasePlugin, WiredPlugin):
    @classmethod
    def type(cls):
        return PluginTypes.OUTPUT

    def __init__(self, channel, config=None):
        WiredPlugin.__init__(channel)
        BasePlugin.__init__(config)

    def run(self):
        raise NotImplementedError()
