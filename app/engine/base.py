import questionary
from questionary import prompt

class BaseEngine:
    connection_questions = [
        {"type": "text", "name": "name", "message": "Connection name:"}
    ]

    reader_questions = []
    writer_questions = []


    def __init__(self, channel, config):
        self.channel = channel
        self.config = config

    @classmethod
    def ask_connection(cls):
        return prompt(cls.connection_questions)

    @classmethod
    def ask_reader(cls):
        return prompt(cls.reader_questions)

    @classmethod
    def ask_writer(cls):
        return prompt(cls.writer_questions)


    def run_query(self):
        raise NotImplementedError()