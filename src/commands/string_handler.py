from .base_handler import BaseCommandHandler

class StringCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "APPEND": self.append_command,
            "STRLEN": self.strlen_command,
            "INCR": self.incr_command,
            "DECR": self.decr_command,
            "INCRBY": self.incrby_command,
            "DECRBY": self.decrby_command,
            "GETRANGE": self.getrange_command,
            "SETRANGE": self.setrange_command,
        }

    def append_command(self, client_id, key, value):
        return str(self.db.string.append(key, value))

    def strlen_command(self, client_id, key):
        return str(self.db.string.strlen(key))

    def incr_command(self, client_id, key):
        return str(self.db.string.incr(key))

    def decr_command(self, client_id, key):
        return str(self.db.string.decr(key))

    def incrby_command(self, client_id, key, increment):
        return str(self.db.string.incrby(key, int(increment)))

    def decrby_command(self, client_id, key, decrement):
        return str(self.db.string.decrby(key, int(decrement)))

    def getrange_command(self, client_id, key, start, end):
        return self.db.string.getrange(key, int(start), int(end))

    def setrange_command(self, client_id, key, offset, value):
        return str(self.db.string.setrange(key, int(offset), value))
