import time
from threading import Lock
from src.logger import setup_logger

logger = setup_logger("rate_limiter")

class RateLimiter:
    def __init__(self, data_store, max_requests, window_seconds):
        self.data_store = data_store
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self.lock = Lock()

    def is_allowed(self, client_id):
        """
        Check if a client's request is allowed under the rate limit.
        Implements the token bucket algorithm.
        """
        with self.lock:
            current_time = int(time.time())
            window_start = current_time - self.window_seconds

            # Remove timestamps outside the window
            self.data_store.zremrangebyscore(client_id, '-inf', window_start)

            # Count requests in the current window
            request_count = self.data_store.zcard(client_id)

            if request_count < self.max_requests:
                # Add current request timestamp
                self.data_store.zadd(client_id, {current_time: current_time})
                logger.info(f"Client '{client_id}' allowed. Request count: {request_count + 1}")
                return True
            logger.warning(f"Client '{client_id}' rate limit exceeded. Request count: {request_count}")
            return False
