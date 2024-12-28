# core/persistence.py

class AOFLogger:
    def __init__(self, file_path="appendonly.aof", sync_interval=1):
        self.file_path = file_path
        self.sync_interval = sync_interval
        self.buffer = []
        self.file = open(file_path, "a")  # Open in append mode

    def log_command(self, command):
        """Log a command to the AOF buffer."""
        self.buffer.append(command + "\n")
        if len(self.buffer) >= self.sync_interval:
            self.sync()

    def sync(self):
        """Write buffered commands to the AOF file."""
        self.file.writelines(self.buffer)
        self.file.flush()
        self.buffer = []

    def replay(self, database):
        """Replay commands from the AOF file to restore the database."""
        try:
            with open(self.file_path, "r") as file:
                for line in file:
                    command_parts = line.strip().split()
                    if command_parts:
                        command = command_parts[0]
                        args = command_parts[1:]
                        if command == "SET":
                            database.set(*args)
                        elif command == "DEL":
                            database.delete(*args)
                        # Add support for other commands if needed
        except FileNotFoundError:
            pass  # AOF file doesn't exist on first startup

    def close(self):
        """Close the AOF file."""
        if self.buffer:
            self.sync()
        self.file.close()
