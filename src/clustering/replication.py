import threading
from typing import List  # Import List
from src.logger import setup_logger
from src.clustering.node import Node
from src.clustering.node_communication import NodeCommunication

logger = setup_logger("replication")

class ReplicationManager:
    def __init__(self, node: Node, communication: NodeCommunication):
        self.node = node
        self.communication = communication
        self.slaves: List[Node] = []
        self.master: Node = None
        self.lock = threading.Lock()

    def set_master(self, master_node: Node):
        with self.lock:
            self.master = master_node
            logger.info(f"Node {self.node.node_id} set to replicate from master {master_node.node_id}")

    def add_slave(self, slave_node: Node):
        with self.lock:
            self.slaves.append(slave_node)
            logger.info(f"Added slave node {slave_node.node_id} to master {self.node.node_id}")

    def remove_slave(self, slave_node_id: str):
        with self.lock:
            self.slaves = [slave for slave in self.slaves if slave.node_id != slave_node_id]
            logger.info(f"Removed slave node {slave_node_id} from master {self.node.node_id}")

    def replicate_command(self, command: str):
        """
        Send the command to all slave nodes for replication.
        """
        with self.lock:
            for slave in self.slaves:
                message = command
                response = self.communication.send_message(slave.host, slave.port, message)
                if response.strip() != "ACK":
                    logger.error(f"Failed to replicate command to slave {slave.node_id}")
                else:
                    logger.info(f"Replicated command to slave {slave.node_id}")

    def handle_master_commands(self, command: str):
        """
        If the node is a master, replicate incoming commands to slaves.
        """
        if self.master is None and self.slaves:
            self.master = self.slaves[0]  # Example: first slave as master
            logger.info(f"Node {self.node.node_id} designated as master.")

        if self.master and self.node.node_id == self.master.node_id:
            self.replicate_command(command)
