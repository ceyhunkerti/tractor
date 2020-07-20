from tqdm import tqdm
from tractor.plugins import BasePlugin, WiredPlugin, PluginType

class OutputPlugin(BasePlugin, WiredPlugin):
    @classmethod
    def type(cls):
        return PluginType.OUTPUT

    def __init__(self, channel, config=None):
        WiredPlugin.__init__(self, channel)
        BasePlugin.__init__(self, config)

        self.pbar = None

    def set_column_count(self, metadata):
        try:
            columns = metadata.get("columns", [])
            if len(columns) > 0:
                self.config["column_count"] = len(columns)
        except:  # pylint: disable=bare-except
            pass

        if not self.config.get("column_count"):
            if not self.config.get("columns"):
                raise ValueError("Unable to determine target columns")
            self.config["column_count"] = len(self.config["columns"])

    def set_metadata(self, metadata):
        self.set_column_count(metadata)
        self.init_progress_bar(metadata.get("count"))


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
