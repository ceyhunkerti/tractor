from questionary import prompt
from app.util import required


class BaseStore:
    categories = ["generic"]

    questions = [
        {
            "type": "text",
            "name": "name",
            "message": "Connection name:",
            "validate": required,
        }
    ]

    @classmethod
    def ask(cls):
        return prompt(cls.questions)

    def __init__(self, config):
        self.config = config
