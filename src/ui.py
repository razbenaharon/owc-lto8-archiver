"""Console and non-interactive UI adapters."""


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


class NonInteractiveUI(ConsoleUI):
    def prompt(self, message, default=None):
        if default is not None:
            return default
        raise RuntimeError(f"Non-interactive operation requires input: {message}")

    def confirm(self, message, expected='yes'):
        raise RuntimeError(f"Non-interactive operation requires confirmation: {message}")
