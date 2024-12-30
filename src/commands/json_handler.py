from .base_handler import BaseCommandHandler

class JSONCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "JSON.SET": self.json_set_command,
            "JSON.GET": self.json_get_command,
            "JSON.DEL": self.json_del_command,
            "JSON.ARRAPPEND": self.json_arrappend_command,
        }

    def json_set_command(self, client_id, *args):
        """Set JSON value. Format: JSON.SET key path value"""
        if len(args) != 3:
            return "ERR wrong number of arguments for 'json.set' command"
            
        key, path, value = args
        result = self.db.json.json_set(key, path, value)
        return result

    def json_get_command(self, client_id, *args):
        """Get JSON value. Format: JSON.GET key [path]"""
        if not args:
            return "ERR wrong number of arguments for 'json.get' command"
            
        key = args[0]
        path = args[1] if len(args) > 1 else '$'
        
        result = self.db.json.json_get(key, path)
        if result is None:
            return "(nil)"
        return result

    def json_del_command(self, client_id, key, path='$'):
        """Delete JSON value. Format: JSON.DEL key [path]"""
        return "(integer) 1" if self.db.json.json_del(key, path) else "(integer) 0"

    def json_arrappend_command(self, client_id, key, path, *values):
        """Append to JSON array. Format: JSON.ARRAPPEND key path value [value ...]"""
        if not values:
            return "ERROR: Wrong number of arguments"
            
        result = self.db.json.json_arrappend(key, path, *values)
        return f"(integer) {result}" if result is not None else "ERROR: Not an array or path not found"
