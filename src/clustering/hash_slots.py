import hashlib

class HashSlots:
    def __init__(self, num_slots=16384):
        self.num_slots = num_slots
        self.slots = {}

    def get_slot(self, key):
        hash_value = int(hashlib.sha256(key.encode('utf-8')).hexdigest(), 16)
        return hash_value % self.num_slots

    def assign_slot(self, key, node):
        slot = self.get_slot(key)
        self.slots[slot] = node

    def get_node(self, key):
        slot = self.get_slot(key)
        return self.slots.get(slot)
