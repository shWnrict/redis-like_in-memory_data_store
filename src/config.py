class Config:
    HOST = "127.0.0.1"
    PORT = 6379
    LOG_LEVEL = "INFO"
    MAX_CLIENTS = 1000
    DATA_LIMIT = 1024 * 1024  # 1 MB per key
    SNAPSHOT_THRESHOLD = 1000  # Number of changes before triggering a snapshot
    TIMEOUT = 60  # Timeout in seconds
    RETRY_ATTEMPTS = 3  # Number of retry attempts
    BACKUP_PATH = "/var/backups"  # Path to store backups
    SAVE_INTERVAL = 300  # Interval in seconds for periodic snapshot saving
    CLEANUP_INTERVAL = 1  # Interval in seconds for cleaning up expired keys
    
    # New clustering configurations
    CLUSTER_HOST = "127.0.0.1"
    CLUSTER_PORT = 7000
    CLUSTER_NODE_ID = "node_1"  # Example node ID
    HASH_SLOT_COUNT = 16384  # Total number of hash slots

    # Memory Management Configurations
    MAX_MEMORY = 1024 * 1024 * 1024  # 1 GB
    MONITOR_INTERVAL = 5  # Interval in seconds for memory monitoring

    # Eviction Policy
    EVICTION_POLICY = "LRU"  # Options: "LRU", "LFU"

    # Rate Limiting Configurations
    RATE_LIMIT_MAX_REQUESTS = 100  # Max requests
    RATE_LIMIT_WINDOW_SECONDS = 60  # Time window in seconds

    # Slow Log Configuration
    SLOWLOG_THRESHOLD_MS = 100  # Threshold in milliseconds for slow commands