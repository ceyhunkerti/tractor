import logging
import csv
import questionary as q

from tractor.util import required
from tractor.plugins.output.base import OutputPlugin
from tractor.plugins import registery


logger = logging.getLogger("plugins.output.csv")


class Csv(OutputPlugin):
    @classmethod
    def ask(cls):
        config = dict()
        config["file"] = q.text("* File", validate=required).ask()
        config["delimiter"] = q.text(
            "* Field delimiter", default=",", validate=required
        ).ask()
        config["lineterminator"] = q.text(
            "* Line terminator", default="\\r\\n", validate=required
        ).ask()
        config["batch_size"] = int(
            q.text("Batch size", default="1000", validate=lambda x: x.isdigit()).ask()
        )
        if not config["batch_size"]:
            config["batch_size"] = 1000

        return config

    def run(self):
        with open(self.config["file"], "w") as handle:
            buffer = []
            writer = csv.writer(
                handle,
                delimiter="{}".format(self.config["delimiter"]),
                lineterminator="{}".format(self.config["lineterminator"]),
            )
            while True:
                message = self.channel.get()
                if message["type"] == self.MessageTypes.METADATA:
                    self.set_metadata(message["content"])
                elif message["type"] == self.MessageTypes.DATA:
                    self.progress(len(message["content"]))
                    if len(message["content"]) < self.config["batch_size"]:
                        buffer += message["content"]
                    else:
                        writer.writerows(buffer)
                        buffer = []
                elif message["type"] == self.MessageTypes.STATUS:
                    if message["content"] == self.Status.SUCCESS:
                        if len(buffer) > 0:
                            writer.writerows(buffer)
                            buffer = []
                    else:
                        logger.error("Received error status from channel")

                    self.close()
                    break


registery.register(Csv)
