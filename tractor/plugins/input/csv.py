import logging
import csv

from tractor.plugins.input.base import InputPlugin
from tractor.plugins import registery
from tractor.util import to_delimiter
from tractor.settings.helpers import parse_boolean

logger = logging.getLogger("plugins.input.csv")


class Csv(InputPlugin):
    """
        file:[required]          = Path to input file
        delimiter:[,]            = Field delimiter
        lineterminator:[\\r\\n]  = Record delimiter
        count:[True]             = Count records and send to output plugin
        header:[False]           = If first row is header
    """

    def count(self):
        delimiter = to_delimiter(self.config.get("delimiter", ","))
        lineterminator = to_delimiter(self.config.get("lineterminator", '\n'))

        with open(self.config["file"], "r") as handle:
            reader = csv.reader(
                handle, delimiter=delimiter, lineterminator=lineterminator,
            )
            count = sum(1 for _ in reader)

        if parse_boolean(self.config.get("header", "true")):
            return count - 1

        return count

    def run(self):
        if parse_boolean(self.config.get("count", "true")):
            self.send_count(self.count())

        with open(self.config["file"], "r") as handle:
            delimiter = to_delimiter(self.config.get("delimiter", ","))
            lineterminator = to_delimiter(self.config.get("lineterminator", "\n"))

            reader = csv.reader(
                handle, delimiter=delimiter, lineterminator=lineterminator,
            )
            if parse_boolean(self.config.get("header", "true")):
                next(reader)

            buffer = []
            for record in reader:
                buffer.append(record)
                if len(buffer) >= self.config.get("batch_size", 1000):
                    self.send_data(buffer)
                    buffer = []

            if len(buffer) > 0:
                self.send_data(buffer)
                buffer = []

            self.done()


registery.register(Csv)
