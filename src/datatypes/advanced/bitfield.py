from typing import List, Tuple, Optional

class BitFieldDataType:
    def __init__(self, database):
        self.db = database

    def _get_bytes(self, key: str) -> bytearray:
        """Get bytes array from string value."""
        try:
            value = self.db.get(key)
            if value is None:
                return bytearray()
            if isinstance(value, bytes):
                return bytearray(value)
            return bytearray(value.encode('latin1', errors='ignore'))
        except Exception:
            return bytearray()

    def _set_bytes(self, key: str, bytes_array: bytearray):
        """Store bytes array as string value."""
        try:
            self.db.store[key] = bytes(bytes_array)
            if not self.db.replaying:
                # Convert to hex for logging to avoid encoding issues
                hex_value = ' '.join(f'{b:02x}' for b in bytes_array)
                self.db.persistence_manager.log_command(f"SET {key} {hex_value}")
        except Exception as e:
            raise ValueError(f"Failed to store bytes: {str(e)}")

    def _get_bits(self, data: bytearray, offset: int, bits: int, unsigned: bool = True) -> int:
        """Extract bits from bytes array starting at bit offset."""
        start_byte = offset >> 3
        end_byte = (offset + bits - 1) >> 3
        bit_offset = offset & 7

        # Ensure we have enough bytes
        if len(data) < end_byte + 1:
            data.extend([0] * (end_byte + 1 - len(data)))

        # Extract relevant bytes
        value = 0
        for i in range(start_byte, end_byte + 1):
            value = (value << 8) | data[i]

        # Shift and mask
        shift = (8 - bits - bit_offset) if end_byte == start_byte else (8 * (end_byte - start_byte + 1) - bits - bit_offset)
        value = (value >> shift) & ((1 << bits) - 1)

        # Handle signed numbers
        if not unsigned and (value & (1 << (bits - 1))):
            value -= (1 << bits)

        return value

    def _set_bits(self, data: bytearray, offset: int, bits: int, value: int, unsigned: bool = True) -> int:
        """Set bits in bytes array starting at bit offset."""
        start_byte = offset >> 3
        end_byte = (offset + bits - 1) >> 3
        bit_offset = offset & 7

        # Ensure we have enough bytes
        if len(data) < end_byte + 1:
            data.extend([0] * (end_byte + 1 - len(data)))

        # Create mask and handle value
        mask = ((1 << bits) - 1)
        value &= mask  # Truncate value to fit bits

        # Position value and mask
        shift = (8 - bits - bit_offset) if end_byte == start_byte else (8 * (end_byte - start_byte + 1) - bits - bit_offset)
        value <<= shift
        mask <<= shift

        # Apply value
        for i in range(start_byte, end_byte + 1):
            byte_mask = mask >> ((end_byte - i) * 8) & 0xFF
            byte_value = value >> ((end_byte - i) * 8) & 0xFF
            data[i] = (data[i] & ~byte_mask) | byte_value

        return value >> shift

    def _parse_offset(self, offset_str: str) -> int:
        """Parse offset that may be hash-based (#N) or numeric."""
        if isinstance(offset_str, int):
            return offset_str
        
        if str(offset_str).startswith('#'):
            try:
                # Hash-based offset: multiply by field width
                multiplier = int(offset_str[1:])
                return multiplier * self._current_bits
            except ValueError:
                raise ValueError(f"Invalid hash offset: {offset_str}")
        
        try:
            return int(offset_str)
        except ValueError:
            raise ValueError(f"Invalid offset: {offset_str}")

    def _parse_type(self, type_spec: str) -> Tuple[bool, int]:
        """Parse type specifier (e.g., 'u8', 'i16') into (unsigned, bits)."""
        if not type_spec or len(type_spec) < 2:
            raise ValueError("Invalid type specifier")

        type_char = type_spec[0].lower()
        if type_char not in ('u', 'i'):
            raise ValueError("Type must be 'u' for unsigned or 'i' for signed")
        
        unsigned = type_char == 'u'
        try:
            bits = int(type_spec[1:])
            # Store current bits for hash-based offsets
            self._current_bits = bits
            if bits <= 0 or bits > 64:
                raise ValueError("Bit width must be between 1 and 64")
            return unsigned, bits
        except ValueError:
            raise ValueError(f"Invalid bit width: {type_spec[1:]}")

    def get(self, key: str, type_spec: str, offset: str) -> Optional[int]:
        """Get integer from bitfield."""
        try:
            unsigned, bits = self._parse_type(type_spec)
            bit_offset = self._parse_offset(offset)
            data = self._get_bytes(key)
            return self._get_bits(data, bit_offset, bits, unsigned)
        except ValueError as e:
            raise ValueError(f"GET error: {str(e)}")

    def set(self, key: str, type_spec: str, offset: str, value: int) -> Optional[int]:
        """Set integer in bitfield."""
        try:
            unsigned, bits = self._parse_type(type_spec)
            bit_offset = self._parse_offset(offset)
            data = self._get_bytes(key)
            old_value = self._get_bits(data, bit_offset, bits, unsigned)
            self._set_bits(data, bit_offset, bits, value, unsigned)
            self._set_bytes(key, data)
            
            if not self.db.replaying:
                self.db.persistence_manager.log_command(f"BITFIELD {key} SET {type_spec} {offset} {value}")
            
            return old_value
        except ValueError as e:
            raise ValueError(f"SET error: {str(e)}")

    def incrby(self, key: str, type_spec: str, offset: str, increment: int) -> Optional[int]:
        """Increment integer in bitfield."""
        try:
            unsigned, bits = self._parse_type(type_spec)
            bit_offset = self._parse_offset(offset)
            data = self._get_bytes(key)
            
            current = self._get_bits(data, bit_offset, bits, unsigned)
            max_val = (1 << bits) if unsigned else (1 << (bits - 1))
            min_val = 0 if unsigned else -(1 << (bits - 1))
            
            # Calculate new value with overflow/underflow handling
            new_val = current + increment
            if unsigned:
                new_val &= (1 << bits) - 1  # Wrap around for unsigned
            else:
                # Handle signed wrap-around
                if new_val >= max_val:
                    new_val = min_val + (new_val - max_val)
                elif new_val < min_val:
                    new_val = max_val - 1 + (new_val - min_val + 1)
                
            self._set_bits(data, bit_offset, bits, new_val, unsigned)
            self._set_bytes(key, data)
            
            if not self.db.replaying:
                self.db.persistence_manager.log_command(f"BITFIELD {key} INCRBY {type_spec} {offset} {increment}")
            
            return new_val
        except ValueError as e:
            raise ValueError(f"INCRBY error: {str(e)}")
