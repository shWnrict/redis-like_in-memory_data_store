import unittest
from src.persistence.aof import AOF

class TestAOF(unittest.TestCase):

    def setUp(self):
        self.aof = AOF()

    def test_append_command(self):
        result = self.aof.append("SET key value")
        self.assertTrue(result)

    def test_load_commands(self):
        self.aof.append("SET key1 value1")
        self.aof.append("SET key2 value2")
        commands = self.aof.load()
        self.assertIn("SET key1 value1", commands)
        self.assertIn("SET key2 value2", commands)

if __name__ == '__main__':
    unittest.main()
