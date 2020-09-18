import os
import yaml
from tractor.settings import CONFIG_FILE, YAML_FILE_ENCODING


class Repository:
    def __init__(self, filename):
        self.filename = filename
        if not os.path.exists(filename):
            open(filename, 'w').close()

        self.snapshot = self.read()

    def read(self):
        with open(self.filename, encoding=YAML_FILE_ENCODING) as handle:
            data = yaml.safe_load(handle.read())

        if not data:
            data = dict()
        if 'mappings' not in data:
            data['mappings'] = []

        return data

    def rollback(self):
        with open(self.filename, 'w+') as handle:
            yaml.dump(self.snapshot, handle)


    def write(self, data):
        self.snapshot = self.read()
        with open(self.filename, 'w+') as handle:
            try:
                yaml.dump(data, handle)
            except TypeError:
                self.rollback()

repo = Repository(CONFIG_FILE)
