import yaml

CONFIG_PATH = "config/app.yml"


class Repository:
    def __init__(self, filename):
        self.filename = filename
        self.snapshot = self.read()

    def read(self):
        with open(self.filename) as handle:
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

repo = Repository(CONFIG_PATH)
