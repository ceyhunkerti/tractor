import questionary
from questionary import prompt

class BaseEngine:
    connection_questions = [
        {"type": "text", "name": "name", "message": "Connection name:"}
    ]

    mapping_source_questions = []
    mapping_target_questions = []

    @classmethod
    def ask_connection(cls):
        return prompt(cls.connection_questions)

    @classmethod
    def ask_mapping_source(cls):
        return prompt(cls.mapping_source_questions)

    @classmethod
    def ask_mapping_target(cls):
        return prompt(cls.mapping_target_questions)


    def run_query(self):
        raise NotImplementedError()