# src/core/transaction_manager.py
from src.logger import setup_logger
from threading import Lock

logger = setup_logger("transaction_manager")

class TransactionManager:
    def __init__(self):
        # Store transaction state for each client
        self.transactions = {}
        self.lock = Lock()

    def start_transaction(self, client_id):
        """
        Start a new transaction for the given client.
        """
        with self.lock:
            if not client_id:
                return "ERR Invalid client ID"
            if client_id in self.transactions:
                return "ERR Transaction already in progress"
            self.transactions[client_id] = []
            logger.info(f"Transaction started for client {client_id}")
            return "OK"

    def queue_command(self, client_id, command):
        """
        Queue a command for the given client.
        """
        with self.lock:
            if client_id not in self.transactions:
                return "ERR No transaction in progress"
            self.transactions[client_id].append(command)
            logger.info(f"Queued command for client {client_id}: {command}")
            return "QUEUED"

    def execute_transaction(self, client_id, data_store, process_command_func):
        """
        Execute all queued commands for the given client atomically.
        """
        with self.lock:
            if client_id not in self.transactions:
                return "ERR No transaction in progress"
            commands = self.transactions.pop(client_id)
        
        results = []
        for command in commands:
            result = process_command_func(command, data_store)
            results.append(result)
        logger.info(f"Executed transaction for client {client_id}")
        return results

    def discard_transaction(self, client_id):
        """
        Discard all queued commands for the given client.
        """
        with self.lock:
            if client_id not in self.transactions:
                return "ERR No transaction in progress"
            self.transactions.pop(client_id)
            logger.info(f"Discarded transaction for client {client_id}")
            return "OK"
