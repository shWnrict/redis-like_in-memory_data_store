# src/datatypes/bitfields.py
import math
import threading
from src.logger import setup_logger  # Import setup_logger

logger = setup_logger("bitfields")  # Initialize logger

class Bitfields:
    def __init__(self, data_store):
        self.store = data_store
        self.lock = threading.Lock()

    def _validate_size(self, size):
        if size < 1 or size > 64:
            return "-ERR Invalid size (must be between 1 and 64)\r\n"
        return None

    def _validate_key(self, key):
        if key not in self.data_store.store:
            return "-ERR Key does not exist\r\n"
        return None

    def _extend_bitfield(self, bitfield, offset, size):
        total_bits = len(bitfield) * 8
        if offset + size > total_bits:
            additional_bytes = math.ceil((offset + size - total_bits) / 8)
            bitfield.extend(bytearray(additional_bytes))

    def get(self, key, offset, size):
        """
        Retrieve a value from the specified offset and size in the bitfield.
        """
        size_error = self._validate_size(size)
        if size_error:
            return size_error

        key_error = self._validate_key(key)
        if key_error:
            return key_error

        bitfield = self.data_store.store[key]
        total_bits = len(bitfield) * 8
        if offset + size > total_bits:
            return "-ERR Out of range\r\n"

        byte_offset = offset // 8
        bit_offset = offset % 8
        mask = (1 << size) - 1

        try:
            value = int.from_bytes(bitfield[byte_offset:byte_offset + math.ceil((bit_offset + size) / 8)], 'big')
            value >>= (8 - ((bit_offset + size) % 8)) % 8
            value &= mask
        except Exception as e:
            return f"-ERR {str(e)}\r\n"

        return f":{value}\r\n"

    def set(self, key, offset, size, value):
        """
        Set a value at the specified offset and size in the bitfield.
        """
        size_error = self._validate_size(size)
        if size_error:
            return size_error

        if key not in self.data_store.store:
            self.data_store.store[key] = bytearray(math.ceil((offset + size) / 8))

        bitfield = self.data_store.store[key]
        self._extend_bitfield(bitfield, offset, size)

        byte_offset = offset // 8
        bit_offset = offset % 8
        mask = (1 << size) - 1

        value &= mask
        value <<= (8 - ((bit_offset + size) % 8)) % 8

        try:
            current_value = int.from_bytes(bitfield[byte_offset:byte_offset + math.ceil((bit_offset + size) / 8)], 'big')
            current_value &= ~(mask << ((8 - ((bit_offset + size) % 8)) % 8))
            current_value |= value

            new_bytes = current_value.to_bytes(math.ceil((bit_offset + size) / 8), 'big')
            bitfield[byte_offset:byte_offset + len(new_bytes)] = new_bytes
        except Exception as e:
            return f"-ERR {str(e)}\r\n"

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

        return self.set(key, offset, size, new_value)

    def handle_command(self, cmd, store, *args):
        if cmd == "BITFIELD":
            return self.bitfield(store, *args)
        return "ERR Unknown BITFIELD command"

    def bitfield(self, store, key: str, *subcommands: str):
        with self.store.lock:
            bitmap = self.store.store.get(key, 0)
            i = 0
            while i < len(subcommands):
                subcmd = subcommands[i].upper()
                if subcmd == "SET":
                    if i + 3 >= len(subcommands):
                        return "ERR BITFIELD SET requires 3 arguments"
                    offset = int(subcommands[i+1])
                    size = int(subcommands[i+2])
                    value = int(subcommands[i+3])
                    bitmap &= ~(((1 << size) - 1) << offset)
                    bitmap |= (value & ((1 << size) - 1)) << offset
                    i += 4
                elif subcmd == "GET":
                    if i + 2 >= len(subcommands):
                        return "ERR BITFIELD GET requires 2 arguments"
                    offset = int(subcommands[i+1])
                    size = int(subcommands[i+2])
                    value = (bitmap >> offset) & ((1 << size) - 1)
                    return value
                elif subcmd == "INCRBY":
                    if i + 3 >= len(subcommands):
                        return "ERR BITFIELD INCRBY requires 3 arguments"
                    offset = int(subcommands[i+1])
                    size = int(subcommands[i+2])
                    increment = int(subcommands[i+3])
                    current = (bitmap >> offset) & ((1 << size) - 1)
                    current += increment
                    bitmap &= ~(((1 << size) - 1) << offset)
                    bitmap |= (current & ((1 << size) - 1)) << offset
                    i += 4
                else:
                    return f"ERR Unknown BITFIELD subcommand {subcmd}"
            self.store.store[key] = bitmap
            logger.info(f"BITFIELD {key} -> {subcommands}")
        return "OK"
