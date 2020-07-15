from app.plugins import BasePlugin, WiredPlugin, PluginTypes

class InputPlugin(BasePlugin, WiredPlugin):
    @classmethod
    def type(cls):
        return PluginTypes.INPUT

    def __init__(self, channel, config=None):
        BasePlugin.__init__(self, config)
        WiredPlugin.__init__(self, channel)

    def done(self):
        self.channel.put(dict(type=self.MessageTypes.STATUS, content=self.Status.DONE))

    def success(self):
        self.channel.put(
            dict(type=self.MessageTypes.STATUS, content=self.Status.SUCCESS)
        )

    def error(self):
        self.channel.put(dict(type=self.MessageTypes.STATUS, content=self.Status.ERROR))

    def send_data(self, content):
        self.channel.put(dict(type=self.MessageTypes.DATA, content=content))

    def send_metadata(self, content):
        self.channel.put(dict(type=self.MessageTypes.METADATA, content=content))

    def run(self):
        raise NotImplementedError()
