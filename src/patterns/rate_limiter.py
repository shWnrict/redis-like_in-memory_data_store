import time

class RateLimiter:
    def __init__(self, data_store, max_requests, window_seconds):
        self.data_store = data_store
        self.max_requests = max_requests
        self.window_seconds = window_seconds

    def is_allowed(self, client_id):
        current_time = int(time.time())
        window_start = current_time - self.window_seconds

        # Remove expired timestamps
        self.data_store.zremrangebyscore(client_id, '-inf', window_start)

        # Count requests in the current window
        request_count = self.data_store.zcard(client_id)

        if request_count < self.max_requests:
            self.data_store.zadd(client_id, {current_time: current_time})
            return True
        else:
            return False
