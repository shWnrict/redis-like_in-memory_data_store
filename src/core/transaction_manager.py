# src/core/transaction_manager.py
from src.logger import setup_logger
from threading import Lock
from typing import List, Dict

logger = setup_logger("transaction_manager")

class TransactionManager:
    def __init__(self):
        # Store transaction state for each client
        self.transactions: Dict[int, List[str]] = {}
        self.lock = Lock()

    def start_transaction(self, client_id: int) -> str:
        """
        Start a new transaction for the given client.
        """
        with self.lock:
            if client_id in self.transactions:
                logger.warning(f"Client {client_id} already has an active transaction.")
                return "-ERR Transaction already started\r\n"
            self.transactions[client_id] = []
            logger.info(f"Started transaction for client {client_id}.")
            return "+OK\r\n"

    def queue_command(self, client_id: int, command: str) -> str:
        """
        Queue a command for the given client.
        """
        with self.lock:
            if client_id not in self.transactions:
                logger.warning(f"Client {client_id} attempted to queue a command without an active transaction.")
                return "-ERR No transaction started\r\n"
            self.transactions[client_id].append(command)
            logger.info(f"Queued command for client {client_id}: {command}")
            return "+QUEUED\r\n"

    def execute_transaction(self, client_id: int, data_store, process_command_func) -> str:
        """
        Execute all queued commands for the given client atomically.
        """
        with self.lock:
            if client_id not in self.transactions:
                logger.warning(f"Client {client_id} attempted to execute a transaction without an active transaction.")
                return "-ERR No transaction started\r\n"
            commands = self.transactions.pop(client_id)
        
        results = []
        for command in commands:
            result = process_command_func(command)
            results.append(result)
            logger.info(f"Executed transaction command for client {client_id}: {command} -> {result}")
        
        logger.info(f"Executed transaction for client {client_id}")
        response = f"*{len(results)}\r\n" + ''.join(results)
        return response

    def discard_transaction(self, client_id: int) -> str:
        """
        Discard all queued commands for the given client.
        """
        with self.lock:
            if client_id not in self.transactions:
                logger.warning(f"Client {client_id} attempted to discard a transaction without an active transaction.")
                return "-ERR No transaction started\r\n"
            self.transactions.pop(client_id)
            logger.info(f"Discarded transaction for client {client_id}.")
            return "+OK\r\n"
