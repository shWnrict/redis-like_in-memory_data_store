# src/protocol.py
import json

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
            elif isinstance(response, (list, dict)):
                # Serialize the data structure to JSON without additional quotes
                json_str = json.dumps(response, ensure_ascii=False, separators=(',', ':'))
                return f"${len(json_str)}\r\n{json_str}\r\n"
            elif isinstance(response, tuple):
                return RESPProtocol.format_response(list(response))
            else:
                raise ValueError(f"Unsupported response type: {type(response)}")
        except Exception as e:
            return f"-ERR {str(e)}\r\n"

    @staticmethod
    def format_command(*args):
        """
        Format a single Redis command using RESP protocol.
        """
        command = f"*{len(args)}\r\n"
        for arg in args:
            if isinstance(arg, bytes):
                arg = arg.decode('utf-8')
            arg_str = str(arg)
            command += f"${len(arg_str)}\r\n{arg_str}\r\n"
        return command

    @staticmethod
    def format_bulk_commands(commands):
        """
        Format multiple Redis commands as a single RESP array.
        """
        bulk_command = ""
        for cmd in commands:
            bulk_command += RESPProtocol.format_command(*cmd)
        return bulk_command

    @staticmethod
    def parse_response(response):
        """
        Parse a single Redis response.
        """
        if not response:
            return None
        prefix = response[0]
        if prefix == '+':
            return response[1:]
        elif prefix == '-':
            return f"ERR {response[1:]}"
        elif prefix == ':':
            return int(response[1:])
        elif prefix == '$':
            length = int(response[1:])
            return response[2:] if length != -1 else None
        elif prefix == '*':
            items = []
            lines = response.split('\r\n')
            count = int(lines[0][1:])
            for i in range(1, count + 1):
                item = lines[i]
                if item.startswith('$'):
                    length = int(item[1:])
                    items.append(lines[i + 1] if length != -1 else None)
                    i += 2
                else:
                    items.append(item)
            return items
        else:
            return response

    @staticmethod
    def parse_bulk_response(response):
        """
        Parse multiple RESP responses from a pipeline execution.
        """
        responses = []
        while response:
            resp, sep, rest = response.partition('\r\n')
            if not sep:
                break
            parsed = RESPProtocol.parse_response(resp + '\r\n')
            responses.append(parsed)
            response = rest
        return responses

class ProtocolError(Exception):
    def __init__(self, message):
        super().__init__(f"RESP Protocol Error: {message}")
