class BitMapDataType:
    """
    A class to represent a bitmap data type for an in-memory data store.
    Attributes:
    -----------
    db : object
        The database instance where the bitmap data is stored.
    Methods:
    --------
    setbit(key: str, offset: int, value: int) -> int:
        Sets or clears a bit at the specified offset in the string value stored at the given key.
    getbit(key: str, offset: int) -> int:
        Gets the bit value at the specified offset in the string value stored at the given key.
    bitcount(key: str, start: int = None, end: int = None, unit: str = 'BYTE') -> int:
        Counts the number of set bits (1s) in the string value stored at the given key, optionally within a specified range.
    bitop(operation: str, dest_key: str, *source_keys: str) -> int:
        Performs a bitwise operation (AND, OR, XOR, NOT) on the string values stored at the source keys and stores the result at the destination key.
    """
    def __init__(self, database):
        self.db = database

    def setbit(self, key: str, offset: int, value: int) -> int:
        """Set or clear a bit at offset in the string."""
        if offset < 0:
            raise ValueError("Offset cannot be negative")
        if value not in (0, 1):
            raise ValueError("Value must be 0 or 1")

        current = self.db.get(key) or ""
        byte_index = offset >> 3  # Divide by 8
        bit_offset = offset & 7   # Modulo 8

        # Extend string if needed
        if byte_index >= len(current):
            current = current.ljust(byte_index + 1, '\x00')

        # Convert to bytes for manipulation
        byte_array = bytearray(current.encode('latin1'))
        byte = byte_array[byte_index]
        
        # Get old bit value
        old_value = (byte >> (7 - bit_offset)) & 1
        
        # Set or clear bit
        if value == 1:
            byte |= 1 << (7 - bit_offset)
        else:
            byte &= ~(1 << (7 - bit_offset))
            
        byte_array[byte_index] = byte
        
        # Store back as string
        self.db.set(key, byte_array.decode('latin1'))
        
        if not self.db.replaying:
            self.db.persistence_manager.log_command(f"SETBIT {key} {offset} {value}")
        
        return old_value

    def getbit(self, key: str, offset: int) -> int:
        """Get bit value at offset in the string."""
        if offset < 0:
            raise ValueError("Offset cannot be negative")

        current = self.db.get(key) or ""
        byte_index = offset >> 3
        bit_offset = offset & 7

        if byte_index >= len(current):
            return 0

        byte = ord(current[byte_index])
        return (byte >> (7 - bit_offset)) & 1

    def bitcount(self, key: str, start: int = None, end: int = None, unit: str = 'BYTE') -> int:
        """
        Count set bits in string.
        unit: 'BYTE' or 'BIT' - specifies whether start/end are byte or bit positions
        """
        current = self.db.get(key) or ""
        if not current:
            return 0

        bytes_array = bytearray(current.encode('latin1'))
        total_bits = len(bytes_array) * 8

        if start is not None and end is not None:
            if unit == 'BIT':
                # Convert bit positions to byte positions
                if start < 0:
                    start = total_bits + start
                if end < 0:
                    end = total_bits + end
                
                start_byte = start >> 3  # Divide by 8
                end_byte = (end + 7) >> 3  # Round up to include partial byte
                
                # Get the relevant bytes
                bytes_array = bytes_array[max(0, start_byte):min(len(bytes_array), end_byte)]
                
                # Count only the bits in range
                count = 0
                for i, byte in enumerate(bytes_array):
                    byte_start_bit = max(0, start - (start_byte + i) * 8) if i == 0 else 0
                    byte_end_bit = min(8, end - (start_byte + i) * 8 + 1) if i == len(bytes_array) - 1 else 8
                    
                    # Create mask for partial byte
                    mask = ((1 << (byte_end_bit - byte_start_bit)) - 1) << (8 - byte_end_bit)
                    masked_byte = byte & mask
                    count += bin(masked_byte).count('1')
                return count
            else:  # BYTE
                if start < 0:
                    start = len(bytes_array) + start
                if end < 0:
                    end = len(bytes_array) + end
                bytes_array = bytes_array[max(0, start):min(len(bytes_array), end + 1)]

        return sum(bin(byte).count('1') for byte in bytes_array)

    def bitop(self, operation: str, dest_key: str, *source_keys: str) -> int:
        """Perform bitwise operation on strings."""
        if not source_keys:
            raise ValueError("No source keys provided")

        # Get all source strings
        sources = []
        max_len = 0
        for key in source_keys:
            value = self.db.get(key) or ""
            sources.append(bytearray(value.encode('latin1')))
            max_len = max(max_len, len(sources[-1]))

        # Pad all strings to max length
        for i in range(len(sources)):
            sources[i] = sources[i].ljust(max_len, b'\x00')

        # Perform operation
        result = bytearray(max_len)
        if operation.upper() == "AND":
            result = sources[0]
            for s in sources[1:]:
                for i in range(max_len):
                    result[i] &= s[i]
        elif operation.upper() == "OR":
            for s in sources:
                for i in range(max_len):
                    result[i] |= s[i]
        elif operation.upper() == "XOR":
            result = sources[0]
            for s in sources[1:]:
                for i in range(max_len):
                    result[i] ^= s[i]
        elif operation.upper() == "NOT":
            if len(source_keys) != 1:
                raise ValueError("NOT operation requires exactly one source key")
            for i in range(max_len):
                result[i] = ~sources[0][i] & 0xFF
        else:
            raise ValueError("Invalid operation")

        # Store result
        self.db.set(dest_key, result.decode('latin1'))
        
        if not self.db.replaying:
            cmd_args = ' '.join([operation, dest_key] + list(source_keys))
            self.db.persistence_manager.log_command(f"BITOP {cmd_args}")
        
        return max_len
