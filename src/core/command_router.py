# src/core/command_router.py
from src.logger import setup_logger

logger = setup_logger("command_router")

class CommandRouter:
    """
    Routes parsed commands to the appropriate server methods.
    """

    def __init__(self, server):
        self.server = server
        self.transaction_queue = []
        self.in_transaction = False

    def route_command(self, command_parts):
        """
        Route the parsed command to the corresponding server method.
        """
        cmd = command_parts[0].upper()
        args = command_parts[1:]

        if cmd == "MULTI":
            self.in_transaction = True
            return "+OK\r\n"
        elif cmd == "EXEC":
            if not self.in_transaction:
                return "-ERR No transaction started\r\n"
            results = []
            for queued_cmd in self.transaction_queue:
                results.append(self.server.process_command(queued_cmd))
            self.transaction_queue.clear()
            self.in_transaction = False
            return "*{}\r\n{}".format(len(results), "".join(results))
        elif self.in_transaction:
            # Queue commands instead of executing immediately
            self.transaction_queue.append(f"{cmd} {' '.join(args)}")
            return "+QUEUED\r\n"

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
