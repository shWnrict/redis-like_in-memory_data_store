import socket
import threading
import time
import pickle
from protocol import format_resp, parse_resp

class ReplicationManager:
    def __init__(self, server):
        self.server = server
        self.is_slave = False
        self.master_host = None
        self.master_port = None
        self.master_socket = None
        self.replication_thread = None
        self.running = True

    def set_as_slave(self, master_host, master_port):
        """Configure server as slave of specified master."""
        self.is_slave = True
        self.master_host = master_host
        self.master_port = master_port
        
        if self.replication_thread:
            self.running = False
            self.replication_thread.join()
            
        self.running = True
        self.replication_thread = threading.Thread(target=self._maintain_replication)
        self.replication_thread.daemon = True
        self.replication_thread.start()
        return True

    def _maintain_replication(self):
        """Maintain connection with master and handle replication."""
        while self.running:
            try:
                if not self._connect_to_master():
                    print("Failed to connect to master, retrying in 1 second...")
                    time.sleep(1)
                    continue

                print("Connected to master, initiating sync...")
                # Initial sync
                self._send_command("SYNC")
                response = self._read_response()
                
                if response and response[0] == "FULLSYNC":
                    if not self._handle_full_sync():
                        print("Full sync failed, retrying...")
                        continue
                
                print("Now receiving real-time updates from master...")
                # Real-time replication
                while self.running and self.master_socket:
                    command = self._read_response()
                    if not command:
                        print("Lost connection to master...")
                        break
                    self._replicate_command(command)
                    
            except Exception as e:
                print(f"Replication error: {e}")
                if self.master_socket:
                    self.master_socket.close()
                    self.master_socket = None
                time.sleep(1)

    def _connect_to_master(self):
        """Establish connection to master server."""
        try:
            if self.master_socket:
                self.master_socket.close()
            
            self.master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.master_socket.connect((self.master_host, self.master_port))
            return True
        except Exception as e:
            print(f"Failed to connect to master: {e}")
            return False

    def _send_command(self, *args):
        """Send command to master."""
        if self.master_socket:
            try:
                command = ' '.join(str(arg) for arg in args)
                self.master_socket.sendall(format_resp(command).encode())
                return True
            except Exception:
                return False
        return False

    def _read_response(self):
        """Read and parse response from master."""
        try:
            data = self.master_socket.recv(4096).decode()
            if data:
                return parse_resp(data)
            return None
        except Exception:
            return None

    def _handle_full_sync(self):
        """Handle full data sync from master."""
        try:
            size_data = self._read_response()
            if not size_data or not size_data[0].isdigit():
                return False
                
            size = int(size_data[0])
            received = 0
            chunks = []
            
            while received < size:
                chunk = self.master_socket.recv(min(4096, size - received))
                if not chunk:
                    return False
                chunks.append(chunk)
                received += len(chunk)
                
            data = pickle.loads(b''.join(chunks))
            self.server.db.restore_from_master(data)
            print(f"Successfully synced with master. Data size: {size} bytes")
            return True
            
        except Exception as e:
            print(f"Full sync error: {e}")
            return False

    def _replicate_command(self, command):
        """Execute replicated command locally."""
        if not command or len(command) < 2:
            return
            
        try:
            cmd_name = command[0].upper()
            args = command[1:]
            
            # Handle SET commands
            if cmd_name == 'SET' and len(args) >= 2:
                self.server.db.set(args[0], args[1])
            # Handle DEL commands
            elif cmd_name == 'DEL' and len(args) >= 1:
                self.server.db.delete(args[0])
            # Handle other commands through command map
            elif cmd_name in self.server.command_map:
                self.server.db.replaying = True
                try:
                    self.server.command_map[cmd_name](None, *args)
                finally:
                    self.server.db.replaying = False
                    
        except Exception as e:
            print(f"Replication command error: {e}")

    def stop(self):
        """Stop replication."""
        self.running = False
        if self.master_socket:
            self.master_socket.close()
            self.master_socket = None
