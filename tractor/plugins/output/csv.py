import logging
import csv

from tractor.plugins.output.base import OutputPlugin
from tractor.plugins import registery
from tractor.util import to_delimiter

logger = logging.getLogger("plugins.output.csv")

class Csv(OutputPlugin):
    """
        file:[required]          = Path to input file
        delimiter:[,]            = Field delimiter
        lineterminator:[\\r\\n]  = Record delimiter
    """

    def run(self):
        self.prepare()

        with open(self.config["file"], "w") as handle:
            delimiter = to_delimiter(self.config.get("delimiter", ","))
            lineterminator = to_delimiter(self.config.get("lineterminator", "\r\n"))

            writer = csv.writer(
                handle, delimiter=delimiter, lineterminator=lineterminator,
            )

            buffer = []
            for message in self.data_channel():
                buffer += message.content
                if len(buffer) >= self.config.get("batch_size", 1000):
                    writer.writerows(buffer)
                    self.progress(len(buffer))
                    buffer = []

            if len(buffer) > 0:
                writer.writerows(buffer)
                self.progress(len(buffer))

        self.close()


registery.register(Csv)
