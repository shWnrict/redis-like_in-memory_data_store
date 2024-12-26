import unittest
import time
from src.core.data_store import DataStore

class TestDataStore(unittest.TestCase):

    def setUp(self):
        self.data_store = DataStore()

    def test_set_and_get(self):
        self.data_store.set('key', 'value')
        self.assertEqual(self.data_store.get('key'), 'value')

    def test_set_with_ttl(self):
        self.data_store.set('key', 'value', ttl=1)
        self.assertEqual(self.data_store.get('key'), 'value')
        time.sleep(1.1)
        self.assertEqual(self.data_store.get('key'), '(nil)')

    def test_delete(self):
        self.data_store.set('key', 'value')
        self.assertEqual(self.data_store.delete('key'), 1)
        self.assertEqual(self.data_store.get('key'), '(nil)')
        self.assertEqual(self.data_store.delete('key'), 0)

    def test_exists(self):
        self.data_store.set('key', 'value')
        self.assertEqual(self.data_store.exists('key'), 1)
        self.data_store.delete('key')
        self.assertEqual(self.data_store.exists('key'), 0)

    def test_incr(self):
        self.data_store.set('key', '1')
        self.assertEqual(self.data_store.incr('key'), 2)
        self.assertEqual(self.data_store.incr('key', 3), 5)

    def test_incr_with_non_integer(self):
        self.data_store.set('key', 'value')
        self.assertEqual(self.data_store.incr('key'), "-ERR Value is not an integer")

    def test_decr(self):
        self.data_store.set('key', '5')
        self.assertEqual(self.data_store.decr('key'), 4)
        self.assertEqual(self.data_store.decr('key', 2), 2)

    def test_decr_with_non_integer(self):
        self.data_store.set('key', 'value')
        self.assertEqual(self.data_store.decr('key'), "-ERR Value is not an integer")

    def test_flushdb(self):
        self.data_store.set('key1', 'value1')
        self.data_store.set('key2', 'value2')
        self.data_store.flushdb()
        self.assertEqual(self.data_store.get('key1'), '(nil)')
        self.assertEqual(self.data_store.get('key2'), '(nil)')

    def test_keys(self):
        self.data_store.set('key1', 'value1')
        self.data_store.set('key2', 'value2')
        self.assertEqual(set(self.data_store.keys()), {'key1', 'key2'})

if __name__ == '__main__':
    unittest.main()
