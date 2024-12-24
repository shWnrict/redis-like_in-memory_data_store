# src/datatypes/bitfields.py
import math

class Bitfields:
    def __init__(self, data_store):
        self.data_store = data_store

    def get(self, key, offset, size):
        """
        Retrieve a value from the specified offset and size in the bitfield.
        """
        if key not in self.data_store.store:
            return ":0\r\n"  # Key doesn't exist

        bitfield = self.data_store.store[key]
        total_bits = len(bitfield) * 8
        if offset + size > total_bits:
            return "-ERR Out of range\r\n"

        byte_offset = offset // 8
        bit_offset = offset % 8
        mask = (1 << size) - 1

        value = int.from_bytes(bitfield[byte_offset:byte_offset + math.ceil((bit_offset + size) / 8)], 'big')
        value >>= (8 - ((bit_offset + size) % 8)) % 8
        value &= mask

        return f":{value}\r\n"

    def set(self, key, offset, size, value):
        """
        Set a value at the specified offset and size in the bitfield.
        """
        if key not in self.data_store.store:
            self.data_store.store[key] = bytearray(math.ceil((offset + size) / 8))

        bitfield = self.data_store.store[key]
        total_bits = len(bitfield) * 8
        if offset + size > total_bits:
            bitfield.extend(bytearray(math.ceil((offset + size - total_bits) / 8)))

        byte_offset = offset // 8
        bit_offset = offset % 8
        mask = (1 << size) - 1

        value &= mask
        value <<= (8 - ((bit_offset + size) % 8)) % 8

        current_value = int.from_bytes(bitfield[byte_offset:byte_offset + math.ceil((bit_offset + size) / 8)], 'big')
        current_value &= ~(mask << ((8 - ((bit_offset + size) % 8)) % 8))
        current_value |= value

        new_bytes = current_value.to_bytes(math.ceil((bit_offset + size) / 8), 'big')
        bitfield[byte_offset:byte_offset + len(new_bytes)] = new_bytes

        return f":{value >> ((8 - ((bit_offset + size) % 8)) % 8)}\r\n"

    def incrby(self, key, offset, size, delta):
        """
        Increment a value at the specified offset and size in the bitfield.
        """
        current_value = self.get(key, offset, size)
        if current_value.startswith("-ERR"):
            return current_value

        current_value = int(current_value[1:].strip())
        new_value = current_value + delta
        if new_value < 0 or new_value >= (1 << size):
            return "-ERR Overflow\r\n"

        self.set(key, offset, size, new_value)
        return f":{new_value}\r\n"
