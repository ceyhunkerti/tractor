import logging
import csv

from tractor.plugins.input.base import InputPlugin
from tractor.plugins import registery
from tractor.util import to_delimiter

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
        delimiter = to_delimiter(self.config["delimiter"])
        lineterminator = to_delimiter(self.config["lineterminator"])

        with open(self.config["file"], "r") as handle:
            reader = csv.reader(
                handle, delimiter=delimiter, lineterminator=lineterminator,
            )
            count = sum(1 for _ in reader)

        if self.config["header"]:
            return count - 1

        return count

    def run(self):
        if self.config["count"]:
            self.send_count(self.count())

        with open(self.config["file"], "r") as handle:
            delimiter = to_delimiter(self.config["delimiter"])
            lineterminator = to_delimiter(self.config["lineterminator"])

            reader = csv.reader(
                handle, delimiter=delimiter, lineterminator=lineterminator,
            )
            if self.config["header"]:
                next(reader)

            buffer = []
            for record in reader:
                buffer.append(record)
                if len(buffer) >= self.config["batch_size"]:
                    self.send_data(buffer)
                    buffer = []

            if len(buffer) > 0:
                self.send_data(buffer)
                buffer = []


registery.register(Csv)
