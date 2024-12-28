# src/protocol.py
import json

class RESPProtocol:
    @staticmethod
    def parse_message(message: str) -> List[List[str]]:
        """Parse RESP message into command list"""
        if not message or not message.startswith("*"):
            return []
        
        try:
            lines = message.strip().split("\r\n")
            commands = []
            i = 0
            while i < len(lines):
                if lines[i].startswith("*"):
                    count = int(lines[i][1:])
                    command = []
                    i += 1
                    for _ in range(count):
                        if lines[i].startswith("$"):
                            length = int(lines[i][1:])
                            i += 1
                            command.append(lines[i][:length])
                        i += 1
                    commands.append(command)
                else:
                    i += 1
            return commands
        except Exception as e:
            logger.error(f"Error parsing RESP message: {e}")
            return []

    @staticmethod
    def format_response(response) -> str:
        """Format response in RESP protocol"""
        if isinstance(response, str):
            if response.startswith("-") or response.startswith("+"):
                return f"{response}\r\n"
            return f"+{response}\r\n"
        elif isinstance(response, int):
            return f":{response}\r\n"
        elif isinstance(response, list):
            if not response:
                return "*0\r\n"
            resp = f"*{len(response)}\r\n"
            for item in response:
                if isinstance(item, (list, tuple)):
                    resp += RESPProtocol.format_response(item)
                else:
                    resp += f"${len(str(item))}\r\n{item}\r\n"
            return resp
        elif response is None:
            return "$-1\r\n"
        else:
            return f"${len(str(response))}\r\n{response}\r\n"

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

    @staticmethod
    def format_array(items):
        resp = f"*{len(items)}\r\n"
        for item in items:
            if isinstance(item, list):
                resp += RESPProtocol.format_array(item)
            elif isinstance(item, tuple):
                resp += f"*2\r\n:{item[0]}\r\n:{item[1]}\r\n"
            else:
                resp += f"${len(str(item))}\r\n{item}\r\n"
        return resp

class ProtocolError(Exception):
    def __init__(self, message):
        super().__init__(f"RESP Protocol Error: {message}")
