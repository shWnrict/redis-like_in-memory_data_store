# src/persistence/aof.py
import os
from src.logger import setup_logger
from src.protocol import RESPProtocol

logger = setup_logger("aof")

class AOF:
    def __init__(self, file_path="appendonly.aof"):
        self.file_path = file_path
        self.logging_enabled = True

    def log_command(self, command):
        if not self.logging_enabled:
            return
        try:
            with open(self.file_path, "a") as f:
                f.write(self._format_command(command) + "\n")
            logger.info(f"AOF: Logged command -> {command}")
        except Exception as e:
            logger.error(f"AOF: Failed to log command -> {command}, Error: {e}")

    def _format_command(self, command):
        """
        Format the command in RESP format for AOF.
        """
        parts = command.split()
        formatted = f"*{len(parts)}\r\n"
        for part in parts:
            formatted += f"${len(part)}\r\n{part}\r\n"
        return formatted

    def truncate(self, snapshot_data):
        try:
            commands = []
            for key, value in snapshot_data.items():
                if isinstance(value, dict):
                    # Handle hash maps
                    cmd_parts = [f"HSET {key}"]
                    for k, v in value.items():
                        cmd_parts.extend([str(k), str(v)])
                    commands.append(" ".join(cmd_parts))
                elif isinstance(value, list):
                    # Handle lists
                    if value:
                        commands.append(f"RPUSH {key} {' '.join(map(str, value))}")
                elif isinstance(value, set):
                    # Handle sets
                    if value:
                        commands.append(f"SADD {key} {' '.join(map(str, value))}")
                else:
                    # Handle strings and other simple types
                    commands.append(f"SET {key} {value}")

            # Clear the file before writing new commands
            with open(self.file_path, "w") as f:
                for cmd in commands:
                    f.write(self._format_command(cmd) + "\n")
            logger.info("AOF: File truncated with snapshot data")
        except Exception as e:
            logger.error(f"AOF: Failed to truncate: {e}")

    def replay(self, process_command_func, data_store):
        """
        Replay commands from the AOF file.
        """
        try:
            if not os.path.exists(self.file_path):
                logger.warning("AOF: No AOF file found, starting fresh")
                return

            self.logging_enabled = False  # Disable logging during replay

            with open(self.file_path, "r") as f:
                data = f.read()
                
            commands = RESPProtocol.parse_message(data)
                
            # Process all commands in a single pass
            for command in commands:
                try:
                    process_command_func(command)
                except Exception as e:
                    logger.error(f"AOF: Error processing command '{command}': {e}")
                    continue

            logger.info("AOF: Replay completed successfully")
        except Exception as e:
            logger.error(f"AOF: Replay failed, Error: {e}")
        finally:
            self.logging_enabled = True  # Re-enable logging after replay