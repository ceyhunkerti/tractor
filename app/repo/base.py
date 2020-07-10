from tinydb import TinyDB
from .storage.yaml_storage import YAMLStorage

# todo move to settings
DB_PATH = "config/app.yml"

class Repository(TinyDB):
    pass


db = Repository(DB_PATH, storage=YAMLStorage)
