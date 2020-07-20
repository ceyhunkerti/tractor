import logging
import csv

from tractor.plugins.output.base import OutputPlugin
from tractor.plugins import registery
from tractor.util import to_delimiter

logger = logging.getLogger("plugins.output.csv")

class Csv(OutputPlugin):

    def _prepare(self):
        if self.config.get('progress', True):
            for message in self.count_channel():
                self.init_progress_bar(message.content)
                break


    def run(self):
        self._prepare()

        with open(self.config["file"], "w") as handle:
            delimiter = to_delimiter(self.config.get("delimiter", ","))
            lineterminator = to_delimiter(self.config.get("lineterminator", "\r\n"))

            writer = csv.writer(
                handle, delimiter=delimiter, lineterminator=lineterminator,
            )

            buffer = []
            for message in self.data_channel():
                if len(buffer) < self.config.get("batch_size", 1000):
                    buffer += message.content
                else:
                    writer.writerows(buffer)
                    self.progress(len(buffer))
                    buffer = []

            if len(buffer) > 0:
                writer.writerows(buffer)
                self.progress(len(buffer))

        self.close()


registery.register(Csv)
