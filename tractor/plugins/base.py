import logging
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional

from tractor.util import slugify
from tractor.settings import (
    META_CHANNEL_TIMEOUT,
    DATA_CHANNEL_TIMEOUT,
    CHANNEL_TIMEOUT,
    COUNT_CHANNEL_TIMEOUT,
)

logger = logging.getLogger("plugins")


class PluginType(Enum):
    INPUT = "input"
    OUTPUT = "output"
    FILTER = "filter"


class BasePlugin(ABC):

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
        self.metadata = None

    @abstractmethod
    def run(self):
        raise NotImplementedError()


class WiredPlugin:
    def __init__(self, channel=None):
        if channel is None:
            raise ValueError("Channel must be given")
        self._channel = channel

    @classmethod
    @abstractmethod
    def name(cls):
        raise NotImplementedError()

    def send_message(self, mtype, content=None):
        self._channel.put(Message(self.name(), mtype, content))

    def task_done(self):
        self._channel.task_done()

    def channel(self, timeout=CHANNEL_TIMEOUT):
        while True:
            message = self._channel.get(timeout=timeout)
            if message.mtype == MessageType.DONE:
                break
            yield message

    def data_channel(self, timeout=DATA_CHANNEL_TIMEOUT):
        while True:
            message = self._channel.get(timeout=timeout)
            if message.mtype == MessageType.DATA:
                yield message
            elif message.mtype == MessageType.DONE:
                break
            else:
                self._channel.put(message)

    def meta_channel(self, timeout=META_CHANNEL_TIMEOUT):
        while True:
            try:
                message = self._channel.get(timeout=timeout)
                if message.mtype == MessageType.METADATA:
                    yield message
                elif message.mtype == MessageType.DONE:
                    break
                else:
                    self._channel.put(message)
            except:  # pylint: disable=bare-except
                break

    def count_channel(self, timeout=COUNT_CHANNEL_TIMEOUT):
        while True:
            try:
                message = self._channel.get(timeout=timeout)
                if message.mtype == MessageType.COUNT:
                    yield message
                elif message.mtype == MessageType.DONE:
                    break
                else:
                    self._channel.put(message)
            except:  # pylint: disable=bare-except
                break


class MessageType(Enum):
    DATA = "data"
    METADATA = "metadata"
    COUNT = "count"
    STATUS = "status"
    DONE = "done"
    SUCCESS = "success"
    ERROR = "error"


@dataclass
class Message:
    sender: BasePlugin
    mtype: MessageType
    content: Optional

    def __init__(self, sender, mtype, content=None):
        super().__init__()
        self.sender = sender
        self.mtype = mtype
        self.content = content
