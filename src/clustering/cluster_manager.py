from .hash_slots import HashSlots
from .node_communication import NodeCommunication
from .replication import ReplicationManager

class ClusterManager:
    def __init__(self, data_store, nodes):
        self.data_store = data_store
        self.hash_slots = HashSlots()
        self.nodes = {node_id: NodeCommunication(host, port) for node_id, (host, port) in nodes.items()}
        self.replication_manager = ReplicationManager(data_store)

    def add_node(self, node_id, host, port):
        self.nodes[node_id] = NodeCommunication(host, port)

    def remove_node(self, node_id):
        if node_id in self.nodes:
            del self.nodes[node_id]

    def assign_slot(self, key, node_id):
        if node_id in self.nodes:
            self.hash_slots.assign_slot(key, self.nodes[node_id])

    def get_node_for_key(self, key):
        return self.hash_slots.get_node(key)

    def replicate_command(self, command):
        self.replication_manager.replicate_command(command)
