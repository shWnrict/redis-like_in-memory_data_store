class MessageQueue:
    def __init__(self, data_store, queue_name):
        self.data_store = data_store
        self.queue_name = queue_name

    def enqueue(self, message):
        self.data_store.rpush(self.queue_name, message)

    def dequeue(self):
        return self.data_store.lpop(self.queue_name)
