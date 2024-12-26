import unittest
from src.server import Server

class TestServer(unittest.TestCase):

    def setUp(self):
        self.server = Server()

    def test_server_initialization(self):
        self.assertIsNotNone(self.server)

    def test_process_command_ping(self):
        result = self.server.process_command("PING")
        self.assertEqual(result, "+PONG\r\n")

    def test_process_command_set_and_get(self):
        self.server.process_command("SET key value")
        result = self.server.process_command("GET key")
        self.assertEqual(result, "$5\r\nvalue\r\n")

    def test_process_command_del(self):
        self.server.process_command("SET key value")
        result = self.server.process_command("DEL key")
        self.assertEqual(result, ":1\r\n")
        result = self.server.process_command("GET key")
        self.assertEqual(result, "$-1\r\n")

    def test_process_command_incr(self):
        self.server.process_command("SET key 1")
        result = self.server.process_command("INCR key")
        self.assertEqual(result, ":2\r\n")

    def test_process_command_decr(self):
        self.server.process_command("SET key 5")
        result = self.server.process_command("DECR key")
        self.assertEqual(result, ":4\r\n")

    def test_process_command_flushdb(self):
        self.server.process_command("SET key1 value1")
        self.server.process_command("SET key2 value2")
        result = self.server.process_command("FLUSHDB")
        self.assertEqual(result, "+OK\r\n")
        result = self.server.process_command("GET key1")
        self.assertEqual(result, "$-1\r\n")
        result = self.server.process_command("GET key2")
        self.assertEqual(result, "$-1\r\n")

    def test_process_command_keys(self):
        self.server.process_command("SET key1 value1")
        self.server.process_command("SET key2 value2")
        result = self.server.process_command("KEYS")
        self.assertIn("key1", result)
        self.assertIn("key2", result)

if __name__ == '__main__':
    unittest.main()
