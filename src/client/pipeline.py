from src.protocol import RESPProtocol  # Ensure RESPProtocol is imported

class Pipeline:
    def __init__(self, connection):
        self.connection = connection
        self.commands = []

    def execute_command(self, *args):
        self.commands.append(args)
        return self

    def execute(self):
        if not self.commands:
            return []

        command = RESPProtocol.format_bulk_commands(self.commands)  # Use RESPProtocol to format bulk commands
        self.connection.send_command_raw(command)  # Add a method to send raw command
        responses = RESPProtocol.parse_bulk_response(self.connection.read_response())  # Parse bulk responses
        self.commands = []
        return responses

    def __getattr__(self, name):
        def method(*args):
            return self.execute_command(name, *args)
        return method
