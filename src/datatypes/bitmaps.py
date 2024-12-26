# src/datatypes/bitmaps.py
from src.logger import setup_logger
import threading

logger = setup_logger("bitmaps")

class Bitmaps:
    def __init__(self):
        self.lock = threading.Lock()

    def setbit(self, store, key, offset, value):
        """
        Sets or clears the bit at the specified offset in a string.
        """
        with self.lock:
            if key not in store:
                store[key] = "\x00"
            if not isinstance(store[key], str):
                return "ERR Key is not a string"

            try:
                offset = int(offset)
                value = int(value)
            except ValueError:
                return "ERR offset or value is not an integer"

            byte_index = offset // 8
            bit_index = offset % 8
            while len(store[key]) <= byte_index:
                store[key] += "\x00"  # Extend the string with null bytes

            current_byte = ord(store[key][byte_index])
            if value == 1:
                new_byte = current_byte | (1 << (7 - bit_index))
            else:
                new_byte = current_byte & ~(1 << (7 - bit_index))

            store[key] = store[key][:byte_index] + chr(new_byte) + store[key][byte_index + 1:]
            logger.info(f"SETBIT {key} {offset} {value}")
            return 1 if (current_byte & (1 << (7 - bit_index))) else 0

    def getbit(self, store, key, offset):
        """
        Gets the bit value at the specified offset in a string.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], str):
                return 0

            try:
                offset = int(offset)
            except ValueError:
                return "ERR offset is not an integer"

            byte_index = offset // 8
            bit_index = offset % 8
            if byte_index >= len(store[key]):
                return 0

            byte = ord(store[key][byte_index])
            bit = (byte >> (7 - bit_index)) & 1
            logger.info(f"GETBIT {key} {offset} -> {bit}")
            return bit

    def bitcount(self, store, key, start=None, end=None):
        """
        Counts the number of set bits (1s) in the string.
        """
        with self.lock:
            if key not in store or not isinstance(store[key], str):
                return 0

            try:
                start = int(start) if start is not None else 0
                end = int(end) if end is not None else len(store[key]) - 1
            except ValueError:
                return "ERR start or end is not an integer"

            count = 0
            for byte in store[key][start:end + 1]:
                count += bin(ord(byte)).count("1")
            logger.info(f"BITCOUNT {key} [{start}:{end}] -> {count}")
            return count

    def bitop(self, store, operation, destkey, *sourcekeys):
        """
        Performs bitwise operations (AND, OR, XOR, NOT) on strings.
        """
        with self.lock:
            if operation.upper() not in {"AND", "OR", "XOR", "NOT"}:
                return "ERR Unknown operation"

            if operation.upper() == "NOT" and len(sourcekeys) != 1:
                return "ERR NOT operation requires exactly one source key"

            max_length = 0
            source_data = []
            for key in sourcekeys:
                if key in store and isinstance(store[key], str):
                    source_data.append(store[key])
                    max_length = max(max_length, len(store[key]))
                else:
                    source_data.append("\x00" * max_length)

            result = ["\x00"] * max_length
            if operation.upper() == "NOT":
                source = source_data[0]
                result = [chr(~ord(c) & 0xFF) for c in source]
            else:
                for i in range(max_length):
                    if operation.upper() == "AND":
                        byte = ord(source_data[0][i]) if i < len(source_data[0]) else 0x00
                        for src in source_data[1:]:
                            byte &= ord(src[i]) if i < len(src) else 0x00
                    elif operation.upper() == "OR":
                        byte = 0x00
                        for src in source_data:
                            byte |= ord(src[i]) if i < len(src) else 0x00
                    elif operation.upper() == "XOR":
                        byte = 0x00
                        for src in source_data:
                            byte ^= ord(src[i]) if i < len(src) else 0x00
                    result[i] = chr(byte)

            store[destkey] = "".join(result)
            logger.info(f"BITOP {operation} {destkey} -> Success")
            return len(store[destkey])
