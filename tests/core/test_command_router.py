import unittest
from unittest.mock import MagicMock
from src.core.command_router import CommandRouter

class TestCommandRouter(unittest.TestCase):

    def setUp(self):
        # Mock server
        self.mock_server = MagicMock()
        self.router = CommandRouter(self.mock_server)

    def test_set_command(self):
        self.router.route_command(["SET", "key", "value"])
        self.mock_server.process_command.assert_called_with("SET key value")

    def test_get_command(self):
        self.router.route_command(["GET", "key"])
        self.mock_server.process_command.assert_called_with("GET key")

    def test_del_command(self):
        self.router.route_command(["DEL", "key"])
        self.mock_server.process_command.assert_called_with("DEL key")

    def test_save_command(self):
        self.router.route_command(["SAVE"])
        self.mock_server.process_command.assert_called_with("SAVE")

    def test_restore_command(self):
        self.router.route_command(["RESTORE"])
        self.mock_server.process_command.assert_called_with("RESTORE")

    def test_incr_command(self):
        self.router.route_command(["INCR", "key"])
        self.mock_server.process_command.assert_called_with("INCR key")

    def test_decr_command(self):
        self.router.route_command(["DECR", "key"])
        self.mock_server.process_command.assert_called_with("DECR key")

    def test_flushdb_command(self):
        self.router.route_command(["FLUSHDB"])
        self.mock_server.process_command.assert_called_with("FLUSHDB")

    def test_keys_command(self):
        self.router.route_command(["KEYS"])
        self.mock_server.process_command.assert_called_with("KEYS")

    def test_unknown_command(self):
        result = self.router.route_command(["UNKNOWN", "cmd"])
        self.assertEqual(result, "-ERR Unknown command UNKNOWN\r\n")

if __name__ == '__main__':
    unittest.main()
