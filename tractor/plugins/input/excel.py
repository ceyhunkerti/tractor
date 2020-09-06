import logging
import csv
import pandas

from tractor.plugins.input.base import InputPlugin
from tractor.plugins import registery
from tractor.util import to_delimiter

logger = logging.getLogger("plugins.input.excel")


class Excel(InputPlugin):
    """
        file:[required]          = Path to input file
        sheet_name:[requeired]   = Sheetname
        columns                  = index of the columns [0, 1, ...]
        count:[True]             = Count records and send to output plugin
        header:[1]               = Index of header row None for no header
    """

    def run(self):

        df = pandas.read_excel(
            self.config["file"],
            sheet_name=self.config["sheet_name"],
            header=self.config["header"],
        )

        self.send_data(df) # todo df to 2d



registery.register(Excel)
