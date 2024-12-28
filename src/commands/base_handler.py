class BaseCommandHandler:
    def __init__(self, database):
        self.db = database

    def get_commands(self):
        """Return a dictionary of command names mapped to their handler methods."""
        return {}
