import logging
import yaml
from tinydb import Storage

logger = logging.getLogger("yaml_storage")

class YAMLStorage(Storage):
    def __init__(self, filename):
        self.snapshot = None
        self.write_error = False
        self.filename = filename

    def read(self):
        with open(self.filename) as handle:
            try:
                data = yaml.safe_load(handle.read())
                return data
            except yaml.YAMLError:
                return None

    def write(self, data):
        logger.debug("Reading snapshot")
        self.snapshot = self.read()
        logger.debug("Writing content")
        with open(self.filename, 'w+') as handle:
            try:
                yaml.dump(data, handle)
            except Exception as error:
                self.rollback()
                raise error

    def rollback(self):
        if self.snapshot is not None:
            with open(self.filename, 'w+') as handle:
                logger.debug('Rolling back')
                yaml.dump(self.snapshot, handle)
                logger.debug('Rolled back')
        else:
            logger.debug("Snapshot is empty")

    def close(self):
        pass
