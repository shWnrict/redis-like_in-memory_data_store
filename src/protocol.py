# Extend src/protocol.py for pipelining
class RESPProtocol:
    @staticmethod
    def parse_message(data):
        """
        Parse one or more RESP messages from the client.
        Supports pipelining by returning a list of commands.
        """
        lines = data.splitlines()
        commands = []
        i = 0

        while i < len(lines):
            if lines[i][0] != "*":
                raise ValueError("Invalid RESP message")
            command_count = int(lines[i][1:])
            command_parts = []
            for _ in range(command_count):
                i += 2  # Skip bulk string length and value lines
                command_parts.append(lines[i])
            commands.append(command_parts)
            i += 1

        return commands

    @staticmethod
    def format_response(response):
        """
        Format a Python object into a RESP-compatible response.
        """
        if isinstance(response, list):
            resp = f"*{len(response)}\r\n"
            for item in response:
                resp += RESPProtocol.format_response(item)
            return resp
        elif response is None:
            return "$-1\r\n"  # Null Bulk String
        elif isinstance(response, str):
            return f"${len(response)}\r\n{response}\r\n"
        elif isinstance(response, int):
            return f":{response}\r\n"
        else:
            raise ValueError("Unsupported response type")
