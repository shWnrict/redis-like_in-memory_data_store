
# src/clustering/node.py
import threading
from typing import Dict, Any
from src.logger import setup_logger

logger = setup_logger("node")

class Node:
    def __init__(self, node_id: str, host: str, port: int):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.status = "active"  # Could be 'active' or 'inactive'
        self.data_store: Dict[str, Any] = {}
        self.lock = threading.Lock()

    def activate(self):
        with self.lock:
            self.status = "active"
            logger.info(f"Node {self.node_id} activated.")

    def deactivate(self):
        with self.lock:
            self.status = "inactive"
            logger.info(f"Node {self.node_id} deactivated.")

    def is_active(self) -> bool:
        with self.lock:
            return self.status == "active"