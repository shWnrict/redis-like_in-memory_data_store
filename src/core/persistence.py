import os
import time
import pickle
import threading

class AOFHandler:
    def __init__(self, database, aof_path="appendonly.aof", sync_interval=1):
        self.database = database
        self.aof_path = aof_path
        self.sync_interval = sync_interval
        self.buffer = []
        self.aof_file = open(aof_path, "a")

    def log_command(self, command):
        """Log a command to the AOF buffer."""
        self.buffer.append(command + "\n")
        if len(self.buffer) >= self.sync_interval:
            self.sync()

    def sync(self):
        """Write buffered commands to the AOF file."""
        if self.buffer:
            self.aof_file.writelines(self.buffer)
            self.aof_file.flush()
            os.fsync(self.aof_file.fileno())
            self.buffer = []

    def replay(self):
        """Replay AOF commands to restore data."""
        try:
            if os.path.exists(self.aof_path):
                with open(self.aof_path, "r") as f:
                    for line in f:
                        command_parts = line.strip().split(maxsplit=2)
                        if command_parts:
                            command = command_parts[0].upper()
                            if command == "SET" and len(command_parts) >= 3:
                                self.database.set(command_parts[1], command_parts[2])
                            elif command == "DEL" and len(command_parts) >= 2:
                                self.database.delete(command_parts[1])
        except Exception as e:
            print(f"Error replaying AOF: {e}")

    def truncate(self):
        """Truncate the AOF file."""
        self.aof_file.close()
        self.aof_file = open(self.aof_path, "w")
        self.aof_file = open(self.aof_path, "a")

    def close(self):
        """Clean shutdown of AOF handler."""
        self.sync()
        if self.aof_file:
            self.aof_file.close()

class SnapshotManager:
    """
    SnapshotManager is responsible for managing the creation and restoration of snapshots for an in-memory database.
    It periodically saves the current state of the database to a file and can restore the database state from a snapshot file.
    
    Attributes:
        snapshot_path (str): The file path where snapshots are saved.
        snapshot_interval (int): The interval (in seconds) at which snapshots are created.
        last_snapshot (float): The timestamp of the last snapshot.
    """
    def __init__(self, database, snapshot_path="snapshot.rdb", snapshot_interval=300):
        self.database = database
        self.snapshot_path = snapshot_path
        self.snapshot_interval = snapshot_interval
        self.last_snapshot = time.time()
        self.running = True
        self.snapshot_thread = threading.Thread(target=self._snapshot_loop, daemon=True)
        self.snapshot_thread.start()
        # Create empty snapshot file if it doesn't exist
        if not os.path.exists(snapshot_path):
            self.create_empty_snapshot()

    def create_empty_snapshot(self):
        """Create an empty snapshot file with initial state."""
        initial_data = {
            'store': {},
            'expiry': {},
            'timestamp': time.time()
        }
        try:
            with open(self.snapshot_path, 'wb') as f:
                pickle.dump(initial_data, f)
        except Exception as e:
            print(f"Error creating initial snapshot: {e}")

    def create_snapshot(self):
        """Create a point-in-time snapshot of the database."""
        temp_path = f"{self.snapshot_path}.tmp"
        try:
            snapshot_data = {
                'store': dict(self.database.store),  # Create a copy
                'expiry': dict(self.database.expiry),
                'timestamp': time.time()
            }
            with open(temp_path, 'wb') as f:
                pickle.dump(snapshot_data, f, protocol=4)
            os.replace(temp_path, self.snapshot_path)
            return True
        except Exception as e:
            print(f"Error creating snapshot: {e}")
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            return False

    def restore_snapshot(self):
        """Restore database state from snapshot."""
        try:
            if not os.path.exists(self.snapshot_path):
                print("No snapshot file found, creating new one")
                self.create_empty_snapshot()
                return 0

            with open(self.snapshot_path, 'rb') as f:
                try:
                    snapshot_data = pickle.load(f)
                    if not isinstance(snapshot_data, dict):
                        raise ValueError("Invalid snapshot format")
                    
                    required_keys = {'store', 'expiry', 'timestamp'}
                    if not all(key in snapshot_data for key in required_keys):
                        raise ValueError("Missing required keys in snapshot")

                    self.database.store = snapshot_data['store']
                    self.database.expiry = snapshot_data['expiry']
                    return snapshot_data['timestamp']
                except (pickle.UnpicklingError, ValueError) as e:
                    print(f"Corrupt snapshot file: {e}")
                    # Create new snapshot file if corrupt
                    self.create_empty_snapshot()
                    return 0
        except Exception as e:
            print(f"Error loading snapshot: {e}")
            self.create_empty_snapshot()
        return 0

    def _snapshot_loop(self):
        """Background thread for creating periodic snapshots."""
        while self.running:
            time.sleep(1)
            if time.time() - self.last_snapshot >= self.snapshot_interval:
                if self.create_snapshot():
                    self.last_snapshot = time.time()

    def stop(self):
        """Stop the snapshot manager."""
        self.running = False
        self.create_snapshot()  # Final snapshot

class PersistenceManager:
    def __init__(self, database):
        self.database = database
        self.aof_handler = AOFHandler(database)
        self.snapshot_manager = SnapshotManager(database)

    def log_command(self, command):
        """Log a command to AOF."""
        self.aof_handler.log_command(command)

    def restore(self):
        """Restore database state from snapshot and AOF."""
        snapshot_time = self.snapshot_manager.restore_snapshot()
        if snapshot_time:
            self.aof_handler.replay()  # Replay commands after snapshot

    def create_snapshot(self):
        """Create a new snapshot and truncate AOF."""
        if self.snapshot_manager.create_snapshot():
            self.aof_handler.truncate()
            return True
        return False

    def close(self):
        """Clean shutdown of persistence."""
        self.snapshot_manager.stop()
        self.aof_handler.close()
