# src/protocol.py
class RESPProtocol:
    @staticmethod
    def parse_message(data):
        commands = []
        # Convert incoming data into a list of lines for incremental processing
        lines = data.splitlines()
        idx = 0

        while idx < len(lines):
            line = lines[idx]
            if not line.startswith("*"):
                raise ProtocolError("Expected array length marker")
            count = int(line[1:])
            idx += 1

            if count < 1:
                raise ProtocolError("Invalid command length")

            command = []
            for _ in range(count):
                length_line = lines[idx]
                idx += 1
                if not length_line.startswith("$"):
                    raise ProtocolError("Expected bulk string marker")
                length = int(length_line[1:])
                bulk_data = lines[idx]
                idx += 1
                if len(bulk_data) != length:
                    raise ProtocolError("Bulk string length mismatch")
                command.append(bulk_data)

            commands.append(command)

        return commands

    @staticmethod
    def format_response(response):
        # Handle single response
        if not isinstance(response, list):
            response = [response]
        
        if len(response) == 1:
            # For single response, don't wrap in array
            return RESPProtocol._format_single_response(response[0])
        else:
            # For multiple responses, use array format
            resp = f"*{len(response)}\r\n"
            for item in response:
                resp += RESPProtocol._format_single_response(item)
            return resp

    @staticmethod
    def _format_single_response(response):
        try:
            if response is None:
                return "$-1\r\n"
            elif isinstance(response, int):
                return f":{response}\r\n"
            elif isinstance(response, bool):
                return f":{1 if response else 0}\r\n"
            elif isinstance(response, float):
                str_val = str(response)
                return f"${len(str_val)}\r\n{str_val}\r\n"
            elif isinstance(response, str):
                if response.startswith(("+", "-", ":", "$", "*")):
                    return response
                elif response == "OK":
                    return "+OK\r\n"
                else:
                    return f"${len(response)}\r\n{response}\r\n"
            else:
                raise ValueError(f"Unsupported response type: {type(response)}")
        except Exception as e:
            return f"-ERR {str(e)}\r\n"

class ProtocolError(Exception):
    def __init__(self, message):
        super().__init__(f"RESP Protocol Error: {message}")
