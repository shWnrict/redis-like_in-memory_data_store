# src/protocol.py
import json

class RESPProtocol:
    @staticmethod
    def parse_message(message: str):
        """
        Parse a RESP message and return a list of commands.
        """
        commands = []
        while message:
            if message.startswith('*'):
                # Array type
                parts = message.split('\r\n')
                array_length = int(parts[0][1:])
                message = '\r\n'.join(parts[1:])
                command = []
                for _ in range(array_length):
                    if message.startswith('$'):
                        dollar_parts = message.split('\r\n', 2)
                        length = int(dollar_parts[0][1:])
                        value = dollar_parts[1]
                        command.append(value)
                        message = dollar_parts[2] if len(dollar_parts) > 2 else ''
                commands.append(command)
            else:
                break
        return commands

    @staticmethod
    def format_response(response):
        """
        Format a response string into RESP format.
        """
        if isinstance(response, str):
            if response.startswith("-ERR"):
                return f"-Error: {response[4:]}\r\n"
            elif response.startswith("+OK"):
                return f"+OK\r\n"
            elif response.startswith(":"):
                return f"{response}\r\n"
            elif response.startswith("$"):
                return f"{response}\r\n"
            else:
                return f"+{response}\r\n"
        elif isinstance(response, int):
            return f":{response}\r\n"
        elif isinstance(response, list):
            formatted = f"*{len(response)}\r\n"
            for item in response:
                if item is None:
                    formatted += "$-1\r\n"
                else:
                    formatted += f"${len(str(item))}\r\n{item}\r\n"
            return formatted
        else:
            return "$-1\r\n"

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
