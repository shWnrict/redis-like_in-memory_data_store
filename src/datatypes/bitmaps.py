# src/datatypes/bitmaps.py
from src.core.data_store import DataStore
from src.logger import setup_logger
from typing import List

logger = setup_logger("bitmaps")

class Bitmaps:
    def __init__(self, store: DataStore, expiry_manager):
        self.store = store
        self.expiry_manager = expiry_manager

    def handle_command(self, cmd, store, *args):
        if cmd == "SETBIT":
            return self.setbit(store, *args)
        elif cmd == "GETBIT":
            return self.getbit(store, *args)
        elif cmd == "BITCOUNT":
            return self.bitcount(store, *args)
        elif cmd == "BITOP":
            return self.bitop(store, *args)
        return "ERR Unknown BITMAP command"

    def setbit(self, store, key: str, offset: int, value: int):
        with self.store.lock:
            bitmap = self.store.store.setdefault(key, 0)
            if value:
                bitmap |= 1 << offset
            else:
                bitmap &= ~(1 << offset)
            self.store.store[key] = bitmap
            logger.info(f"SETBIT {key} {offset} {value}")
        return value

    def getbit(self, store, key: str, offset: int):
        with self.store.lock:
            bitmap = self.store.store.get(key, 0)
            bit = (bitmap >> offset) & 1
            logger.info(f"GETBIT {key} {offset} -> {bit}")
        return bit

    def bitcount(self, store, key: str, start: int = 0, end: int = -1):
        with self.store.lock:
            bitmap = self.store.store.get(key, 0)
            binary = bin(bitmap)
            count = binary.count('1')
            logger.info(f"BITCOUNT {key} -> {count}")
        return count

    def bitop(self, store, operation: str, dest_key: str, *src_keys: str):
        with self.store.lock:
            result = 0
            if operation == "AND":
                result = self.store.store.get(src_keys[0], 0)
                for key in src_keys[1:]:
                    result &= self.store.store.get(key, 0)
            elif operation == "OR":
                for key in src_keys:
                    result |= self.store.store.get(key, 0)
            elif operation == "XOR":
                for key in src_keys:
                    result ^= self.store.store.get(key, 0)
            elif operation == "NOT":
                if len(src_keys) != 1:
                    return "ERR NOT operation requires exactly one source key"
                result = ~self.store.store.get(src_keys[0], 0)
            else:
                return "ERR Unknown BITOP operation"
            self.store.store[dest_key] = result
            logger.info(f"BITOP {operation} {dest_key} {' '.join(src_keys)}")
        return len(bin(result))  # Return the length of the resulting bitmap
