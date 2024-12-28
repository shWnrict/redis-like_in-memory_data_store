from .base_handler import BaseCommandHandler

class CoreCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "SET": self.set_command,
            "GET": self.get_command,
            "DEL": self.del_command,
            "EXISTS": self.exists_command,
        }

    def set_command(self, client_id, key, value):
        self.db.set(key, value)
        return "OK"

    def get_command(self, client_id, key):
        value = self.db.get(key)
        return value if value else "(nil)"

    def del_command(self, client_id, key):
        return "(1)" if self.db.delete(key) else "(0)"

    def exists_command(self, client_id, key):
        return "(1)" if self.db.exists(key) else "(0)"
