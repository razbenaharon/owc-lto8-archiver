"""Console UI adapter."""


class ConsoleUI:
    def info(self, message=''):
        print(message)

    def warning(self, message):
        print(message)

    def error(self, message):
        print(message)

    def prompt(self, message, default=None):
        value = input(message)
        if value == '' and default is not None:
            return default
        return value

    def confirm(self, message, expected='yes'):
        return self.prompt(message).strip().lower() == expected.lower()
