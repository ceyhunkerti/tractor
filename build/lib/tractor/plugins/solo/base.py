from tractor.plugins import BasePlugin, PluginType

class SoloPlugin(BasePlugin):

    @classmethod
    def type(cls):
        return PluginType.SOLO

    def run(self):
        raise NotImplementedError()
