# Extend src/protocol.py for pipelining
class RESPProtocol:
    @staticmethod
    def parse_message(data):
        try:
            lines = data.splitlines()
            if not lines or not lines[0].startswith("*"):
                raise ValueError("Invalid RESP format")
                
            commands = []
            i = 0
            while i < len(lines):
                if not lines[i].startswith("*"):
                    raise ValueError("Expected array length marker")
                count = int(lines[i][1:])
                if count < 1:
                    raise ValueError("Invalid command length")
                    
                command = []
                for _ in range(count):
                    i += 1
                    if i >= len(lines) or not lines[i].startswith("$"):
                        raise ValueError("Invalid bulk string")
                        
                    i += 1
                    if i >= len(lines):
                        raise ValueError("Incomplete command")
                    command.append(lines[i])
                    
                commands.append(command)
                i += 1
                
            return commands
        except (ValueError, IndexError) as e:
            raise ProtocolError(f"Protocol parsing error: {e}")
    
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

class ProtocolError(Exception):
    pass