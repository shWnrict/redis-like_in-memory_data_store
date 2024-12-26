import unittest
from src.core.transaction_manager import TransactionManager

class TestTransactionManager(unittest.TestCase):

    def setUp(self):
        self.transaction_manager = TransactionManager()

    def test_begin_transaction(self):
        result = self.transaction_manager.begin()
        self.assertEqual(result, "OK")

    def test_commit_transaction(self):
        self.transaction_manager.begin()
        result = self.transaction_manager.commit()
        self.assertEqual(result, "OK")

    def test_rollback_transaction(self):
        self.transaction_manager.begin()
        result = self.transaction_manager.rollback()
        self.assertEqual(result, "OK")

    def test_transaction_commands(self):
        self.transaction_manager.begin()
        self.transaction_manager.queue_command("SET key value")
        self.transaction_manager.queue_command("GET key")
        result = self.transaction_manager.commit()
        self.assertEqual(result, ["SET key value", "GET key"])

if __name__ == '__main__':
    unittest.main()
