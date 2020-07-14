from app.plugins import BasePlugin, PluginTypes

class SoloPlugin(BasePlugin):

    @classmethod
    def type(cls):
        return PluginTypes.SOLO

    def run(self):
        raise NotImplementedError()
