# src/persistence/aof.py
from src.logger import setup_logger

logger = setup_logger("aof")

class AOF:
    def __init__(self, file_path="appendonly.aof"):
        self.file_path = file_path

    def log_command(self, command):
        """
        Log a write command to the AOF file.
        """
        try:
            with open(self.file_path, "a") as f:
                f.write(command + "\n")
            logger.info(f"AOF: Logged command -> {command}")
        except Exception as e:
            logger.error(f"AOF: Failed to log command -> {command}, Error: {e}")

    def truncate(self, snapshot_data):
        """
        Rewrite the AOF file using the latest snapshot data.
        """
        try:
            with open(self.file_path, "w") as f:
                for key, value in snapshot_data.items():
                    if isinstance(value, dict):
                        f.write(f"HSET {key} {' '.join(f'{k} {v}' for k, v in value.items())}\n")
                    elif isinstance(value, list):
                        f.write(f"LPUSH {key} {' '.join(map(str, value[::-1]))}\n")
                    else:
                        f.write(f"SET {key} {value}\n")
            logger.info("AOF: File truncated with snapshot data")
        except Exception as e:
            logger.error(f"AOF: Failed to truncate: {e}")

    def replay(self, process_command_func, data_store):
        """
        Replay the AOF file to restore the data store.
        """
        try:
            with open(self.file_path, "r") as f:
                commands = f.readlines()

            for command in commands:
                process_command_func(command.strip(), data_store)

            logger.info("AOF: Replay completed successfully")
        except FileNotFoundError:
            logger.warning("AOF: No AOF file found, starting fresh")
        except Exception as e:
            logger.error(f"AOF: Replay failed, Error: {e}")
