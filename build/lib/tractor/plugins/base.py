
from enum import Enum
from types import SimpleNamespace
from questionary import prompt
from slugify import slugify


class PluginType(Enum):
    INPUT = "input"
    OUTPUT = "output"
    SOLO = "solo"
    FILTER = "filter"


class BasePlugin:
    questions = []

    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    def type(cls):
        raise NotImplementedError()

    @classmethod
    def slug(cls):
        return slugify(f"{cls.type()} {cls.name()}")

    @classmethod
    def enabled(cls):
        return True

    def __init__(self, config=None):
        self.config = config

    @classmethod
    def ask(cls):
        prompt(cls.questions)

    def run(self):
        raise NotImplementedError()


class WiredPlugin:
    MessageTypes = SimpleNamespace(DATA="DATA", METADATA="METADATA", STATUS="STATUS")
    Status = SimpleNamespace(DONE="DONE", SUCCESS="SUCCESS", ERROR="ERROR")

    def __init__(self, channel=None):
        if channel is None:
            raise ValueError("Channel must be given")

        self.channel = channel



class MessageType(Enum):
    DATA = "data"
    METADATA = "metadata"
    STATUS = "status"
    DONE = "done"
    ERROR = "error"
    SUCCESS = "success"


@dataclass
class Message:
    message_type: MessageType
