import threading

class ReplicationManager:
    def __init__(self, data_store):
        self.data_store = data_store
        self.slaves = []
        self.lock = threading.Lock()

    def add_slave(self, slave_connection):
        with self.lock:
            self.slaves.append(slave_connection)

    def remove_slave(self, slave_connection):
        with self.lock:
            self.slaves.remove(slave_connection)

    def replicate_command(self, command):
        with self.lock:
            for slave in self.slaves:
                slave.execute(command)
