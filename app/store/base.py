import questionary
from questionary import prompt

class BaseStore:
    categories = ['generic']

    questions = [
        {"type": "text", "name": "name", "message": "Connection name:"}
    ]

    @classmethod
    def ask(cls):
        return prompt(cls.questions)

    def __init__(self, config):
        self.config = config
