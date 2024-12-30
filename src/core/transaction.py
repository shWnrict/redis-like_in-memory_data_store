# core/transaction.py

class TransactionManager:
    """
    TransactionManager is a class that manages transactions for a database. It allows clients to start, queue commands, 
    execute, and discard transactions. Transactions ensure that a series of commands are executed atomically, meaning 
    either all commands are executed successfully, or none are executed at all.
  
    """
    def __init__(self, database):
        self.database = database
        self.transactions = {}  # {client_id: [commands]}
        self.in_transaction = set()  # Set of clients in MULTI state

    def start_transaction(self, client_id):
        """Start a transaction for the given client."""
        if client_id in self.in_transaction:
            return "ERROR: Transaction already in progress"
        self.in_transaction.add(client_id)
        self.transactions[client_id] = []
        return "OK"

    def queue_command(self, client_id, command, *args):
        """Queue a command for execution in the transaction."""
        if client_id not in self.in_transaction:
            return None  # Not in transaction, execute normally
        if command in ["MULTI", "EXEC", "DISCARD"]:
            return None  # These commands are not queued
        self.transactions[client_id].append((command, args))
        return "QUEUED"

    def execute_transaction(self, client_id):
        """Execute all queued commands for the transaction atomically."""
        if client_id not in self.in_transaction:
            return "ERROR: No transaction in progress"
        
        try:
            results = []
            commands = self.transactions[client_id]
            
            # Execute commands atomically
            for command, args in commands:
                command_func = self.database.command_map.get(command)
                if command_func:
                    # Pass client_id and unpack args properly
                    if isinstance(args, (list, tuple)):
                        result = command_func(client_id, *args)
                    else:
                        result = command_func(client_id, args)
                    results.append(result)
                else:
                    results.append(f"ERROR: Unknown command {command}")
            
            # Clean up transaction state
            self.in_transaction.remove(client_id)
            del self.transactions[client_id]
            return results if results else ["OK"]

        except Exception as e:
            # If any command fails, discard the transaction
            self.discard_transaction(client_id)
            return f"ERROR: Transaction failed - {str(e)}"

    def discard_transaction(self, client_id):
        """Discard all queued commands for the transaction."""
        if client_id not in self.in_transaction:
            return "ERROR: No transaction in progress"
        self.in_transaction.remove(client_id)
        del self.transactions[client_id]
        return "OK"

    def is_in_transaction(self, client_id):
        """Check if a client is in a transaction."""
        return client_id in self.in_transaction
