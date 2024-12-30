from .base_handler import BaseCommandHandler

class CoreCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "SET": self.set_command,
            "GET": self.get_command,
            "DEL": self.del_command,
            "EXISTS": self.exists_command,
            "EXPIRE": self.expire_command,    # Add expiry commands
            "TTL": self.ttl_command,
            "PERSIST": self.persist_command,
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

    def expire_command(self, client_id, key, seconds):
        """Set a timeout on key."""
        try:
            seconds = int(seconds)
            return 1 if self.db.expiry_manager.set_expiry(key, seconds) else 0
        except ValueError:
            return "ERR value is not an integer or out of range"

    def ttl_command(self, client_id, key):
        """Get the time to live for a key in seconds."""
        return self.db.expiry_manager.ttl(key)

    def persist_command(self, client_id, key):
        """Remove the expiration from a key."""
        return 1 if self.db.expiry_manager.persist(key) else 0
