from .base_handler import BaseCommandHandler

class JSONCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "JSON.SET": self.json_set_command,
            "JSON.GET": self.json_get_command,
            "JSON.DEL": self.json_del_command,
            "JSON.ARRAPPEND": self.json_arrappend_command,
        }

    def json_set_command(self, client_id, key, path, value, *args):
        """Set JSON value. Format: JSON.SET key path value [NX|XX]"""
        nx = False
        xx = False
        
        for arg in args:
            if arg.upper() == 'NX':
                nx = True
            elif arg.upper() == 'XX':
                xx = True
                
        if nx and xx:
            return "ERROR: NX and XX are mutually exclusive"
            
        success = self.db.json.json_set(key, path, value, nx=nx, xx=xx)
        return "OK" if success else "ERROR"

    def json_get_command(self, client_id, key, path='$'):
        """Get JSON value. Format: JSON.GET key [path]"""
        result = self.db.json.json_get(key, path)
        return result if result is not None else "(nil)"

    def json_del_command(self, client_id, key, path='$'):
        """Delete JSON value. Format: JSON.DEL key [path]"""
        return "(integer) 1" if self.db.json.json_del(key, path) else "(integer) 0"

    def json_arrappend_command(self, client_id, key, path, *values):
        """Append to JSON array. Format: JSON.ARRAPPEND key path value [value ...]"""
        if not values:
            return "ERROR: Wrong number of arguments"
            
        result = self.db.json.json_arrappend(key, path, *values)
        return f"(integer) {result}" if result is not None else "ERROR: Not an array or path not found"
