import hashlib
from typing import Dict, List
from src.logger import setup_logger
from src.clustering.node import Node
import threading

logger = setup_logger("hash_slots")

class HashSlots:
    TOTAL_SLOTS = 16384  # Redis uses 16384 hash slots

    def __init__(self):
        self.slot_to_node: Dict[int, Node] = {}
        self.nodes: List[Node] = []
        self.lock = threading.Lock()

    def add_node(self, node: Node):
        with self.lock:
            self.nodes.append(node)
            self._rebalance_slots()
            logger.info(f"Added node {node.node_id} and rebalanced slots.")

    def remove_node(self, node_id: str):
        with self.lock:
            self.nodes = [node for node in self.nodes if node.node_id != node_id]
            self._rebalance_slots()
            logger.info(f"Removed node {node_id} and rebalanced slots.")

    def _rebalance_slots(self):
        if not self.nodes:
            self.slot_to_node = {}
            return

        slots_per_node = self.TOTAL_SLOTS // len(self.nodes)
        extra_slots = self.TOTAL_SLOTS % len(self.nodes)
        current_slot = 0

        self.slot_to_node = {}
        for i, node in enumerate(self.nodes):
            assigned_slots = slots_per_node + (1 if i < extra_slots else 0)
            for _ in range(assigned_slots):
                if current_slot >= self.TOTAL_SLOTS:
                    break
                self.slot_to_node[current_slot] = node
                current_slot += 1
        logger.info("Rebalanced hash slots among nodes.")

    def get_node(self, key: str) -> Node:
        slot = self._compute_slot(key)
        with self.lock:
            node = self.slot_to_node.get(slot)
            if node and node.is_active():
                return node
            logger.warning(f"No active node found for slot {slot}.")
            return None

    def _compute_slot(self, key: str) -> int:
        """
        Compute the hash slot for a given key using CRC16.
        """
        key_hash = hashlib.sha1(key.encode()).hexdigest()
        slot = int(key_hash, 16) % self.TOTAL_SLOTS
        logger.debug(f"Computed slot {slot} for key '{key}'.")
        return slot

    def get_all_nodes(self) -> List[Node]:
        with self.lock:
            return list(self.nodes)
