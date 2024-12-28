# core/transaction.py

class TransactionManager:
    def __init__(self, database):
        self.database = database
        self.transactions = {}

    def start_transaction(self, client_id):
        """Start a transaction for the given client."""
        if client_id in self.transactions:
            return "ERROR: Transaction already in progress"
        self.transactions[client_id] = []

    def queue_command(self, client_id, command):
        """Queue a command for execution in the transaction."""
        if client_id not in self.transactions:
            return "ERROR: No transaction in progress"
        self.transactions[client_id].append(command)

    def execute_transaction(self, client_id):
        """Execute all queued commands for the transaction."""
        if client_id not in self.transactions:
            return "ERROR: No transaction in progress"
        results = []
        for command in self.transactions[client_id]:
            try:
                results.append(command())
            except Exception as e:
                results.append(f"ERROR: {e}")
        del self.transactions[client_id]
        return results

    def discard_transaction(self, client_id):
        """Discard all queued commands for the transaction."""
        if client_id not in self.transactions:
            return "ERROR: No transaction in progress"
        del self.transactions[client_id]
