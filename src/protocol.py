# src/protocol.py
class RESPProtocol:
    @staticmethod
    def parse_message(data):
        commands = []
        lines = data.split('\r\n')
        i = 0
        
        while i < len(lines):
            line = lines[i]
            if not line:
                i += 1
                continue
                
            if line[0] == '*':
                count = int(line[1:])
                command = []
                i += 1
                
                for _ in range(count):
                    if i >= len(lines):
                        raise ProtocolError("Incomplete command")
                    
                    bulk_len = lines[i]
                    if not bulk_len.startswith('$'):
                        raise ProtocolError("Expected bulk string marker")
                    
                    length = int(bulk_len[1:])
                    i += 1
                    
                    if i >= len(lines):
                        raise ProtocolError("Incomplete command")
                    
                    command.append(lines[i])
                    i += 1
                    
                commands.append(command)
            else:
                i += 1
                
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
