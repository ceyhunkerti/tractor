from tqdm import tqdm
from app.plugins import BasePlugin, WiredPlugin, PluginTypes

class OutputPlugin(BasePlugin, WiredPlugin):
    @classmethod
    def type(cls):
        return PluginTypes.OUTPUT

    def __init__(self, channel, config=None):
        WiredPlugin.__init__(self, channel)
        BasePlugin.__init__(self, config)

        self.pbar = None

    def progress(self, step):
        if self.pbar:
            self.pbar.update(step)

    def close(self):
        if self.pbar:
            self.pbar.close()

        self.channel.task_done()

    def init_progress_bar(self, count):
        self.pbar = tqdm(total=count, desc=self.__class__.__name__)


    def run(self):
        raise NotImplementedError()
