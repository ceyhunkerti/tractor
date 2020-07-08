from questionary import prompt

class BaseEngine:
    questions = []

    @classmethod
    def category(cls):
        return 'generic'

    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    def type(cls):
        return cls.__name__.lower()

    @classmethod
    def enabled(cls):
        return True

    def __init__(self, *args, **kwargs):
        pass

    @classmethod
    def ask(cls):
        prompt(cls.questions)