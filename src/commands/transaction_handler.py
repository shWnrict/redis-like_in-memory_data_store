from .base_handler import BaseCommandHandler

class TransactionCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "MULTI": self.multi_command,
            "EXEC": self.exec_command,
            "DISCARD": self.discard_command,
        }

    def multi_command(self, client_id):
        result = self.db.transaction_manager.start_transaction(client_id)
        if isinstance(result, str) and result.startswith("ERR"):
            return result
        return "OK"

    def exec_command(self, client_id):
        result = self.db.transaction_manager.execute_transaction(client_id)
        if isinstance(result, str) and result.startswith("ERR"):
            return result
        return result

    def discard_command(self, client_id):
        result = self.db.transaction_manager.discard_transaction(client_id)
        if isinstance(result, str) and result.startswith("ERR"):
            return result
        return "OK"
