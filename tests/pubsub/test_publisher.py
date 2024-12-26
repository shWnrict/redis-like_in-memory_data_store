import unittest
from src.pubsub.publisher import Publisher
from src.pubsub.subscriber import Subscriber

class TestPublisher(unittest.TestCase):

    def setUp(self):
        self.publisher = Publisher()
        self.subscriber = Subscriber()

    def test_publish_message(self):
        result = self.publisher.publish("channel", "message")
        self.assertEqual(result, 0)  # Assuming no subscribers

    def test_subscribe_and_publish(self):
        self.subscriber.subscribe("channel")
        self.publisher.add_subscriber(self.subscriber)
        result = self.publisher.publish("channel", "message")
        self.assertEqual(result, 1)  # One subscriber

    def test_unsubscribe_and_publish(self):
        self.subscriber.subscribe("channel")
        self.publisher.add_subscriber(self.subscriber)
        self.subscriber.unsubscribe("channel")
        result = self.publisher.publish("channel", "message")
        self.assertEqual(result, 0)  # No subscribers

if __name__ == '__main__':
    unittest.main()
