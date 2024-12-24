class Config:
    HOST = "127.0.0.1"
    PORT = 6379
    LOG_LEVEL = "INFO"
    MAX_CLIENTS = 1000
    DATA_LIMIT = 1024 * 1024  # 1 MB per key
    SNAPSHOT_THRESHOLD = 1000  # Number of changes before triggering a snapshot