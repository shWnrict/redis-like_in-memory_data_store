import unittest
from src.protocol import Protocol

class TestProtocol(unittest.TestCase):

    def setUp(self):
        self.protocol = Protocol()

    def test_parse_command_ping(self):
        result = self.protocol.parse_command("PING")
        self.assertEqual(result, ["PING"])

    def test_parse_command_set(self):
        result = self.protocol.parse_command("SET key value")
        self.assertEqual(result, ["SET", "key", "value"])

    def test_parse_command_get(self):
        result = self.protocol.parse_command("GET key")
        self.assertEqual(result, ["GET", "key"])

    def test_parse_command_with_extra_spaces(self):
        result = self.protocol.parse_command("  SET   key   value  ")
        self.assertEqual(result, ["SET", "key", "value"])

    def test_parse_command_with_newlines(self):
        result = self.protocol.parse_command("SET key value\n")
        self.assertEqual(result, ["SET", "key", "value"])

if __name__ == '__main__':
    unittest.main()
