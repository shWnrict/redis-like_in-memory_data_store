class MessageQueue:
    def __init__(self, data_store, queue_name):
        self.data_store = data_store
        self.queue_name = queue_name

    def enqueue(self, message):
        self.data_store.lpush(self.queue_name, message)
        self.data_store.expire(self.queue_name, ttl=3600)  # Set a TTL of 1 hour for the queue

    def dequeue(self):
        message = self.data_store.rpop(self.queue_name)
        return message if message else "(nil)"

    def peek(self):
        message = self.data_store.lindex(self.queue_name, -1)
        return message if message else "(nil)"
