# src/core/command_router.py
from src.logger import setup_logger

logger = setup_logger("command_router")

class CommandRouter:
    """
    Routes parsed commands to the appropriate server methods.
    """

    def __init__(self, server):
        self.server = server

    def route_command(self, command_parts):
        """
        Route the parsed command to the corresponding server method.
        """
        cmd = command_parts[0].upper()
        args = command_parts[1:]

        if cmd == "SET":
            return self.server.process_command(f"SET {' '.join(args)}")
        elif cmd == "GET":
            return self.server.process_command(f"GET {' '.join(args)}")
        elif cmd == "DEL":
            return self.server.process_command(f"DEL {' '.join(args)}")
        elif cmd == "SAVE":
            return self.server.process_command("SAVE")
        elif cmd == "RESTORE":
            return self.server.process_command("RESTORE")
        else:
            return f"-ERR Unknown command {cmd}\r\n"
