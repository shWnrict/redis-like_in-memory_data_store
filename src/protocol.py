class RESPParser:
    """Parses RESP commands into Python objects."""

    def parse(self, data):
        """Parse RESP-encoded data into a Python object."""
        if not data:
            raise ValueError("Empty data received.")
        
        first_byte = data[0:1]
        if first_byte == b"+":
            return self._parse_simple_string(data)
        elif first_byte == b"-":
            return self._parse_error(data)
        elif first_byte == b":":
            return self._parse_integer(data)
        elif first_byte == b"$":
            return self._parse_bulk_string(data)
        elif first_byte == b"*":
            return self._parse_array(data)
        else:
            raise ValueError("Invalid RESP format.")

    def _parse_simple_string(self, data):
        """Parse RESP Simple Strings."""
        return data[1:].strip().decode()

    def _parse_error(self, data):
        """Parse RESP Errors."""
        return f"ERROR: {data[1:].strip().decode()}"

    def _parse_integer(self, data):
        """Parse RESP Integers."""
        return int(data[1:].strip())

    def _parse_bulk_string(self, data):
        """Parse RESP Bulk Strings."""
        lines = data.split(b"\r\n")
        length = int(lines[0][1:])
        if length == -1:
            return None  # Null Bulk String
        return lines[1].decode()

    def _parse_array(self, data):
        """Parse RESP Arrays."""
        lines = data.split(b"\r\n")
        length = int(lines[0][1:])
        if length == -1:
            return None  # Null Array
        array = []
        index = 1
        while length > 0:
            element = self.parse(lines[index].encode() + b"\r\n")
            array.append(element)
            index += 1
            length -= 1
        return array
