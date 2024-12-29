from .base_handler import BaseCommandHandler
import time

class TimeSeriesCommandHandler(BaseCommandHandler):
    def get_commands(self):
        return {
            "TS.CREATE": self.ts_create_command,
            "TS.ADD": self.ts_add_command,
            "TS.GET": self.ts_get_command,
            "TS.RANGE": self.ts_range_command,
        }

    def ts_create_command(self, client_id, key, *args):
        """Create a new time series. Format: TS.CREATE key [RETENTION retentionms] [DUPLICATE_POLICY policy] [LABELS label value..]"""
        retention_ms = 0
        duplicate_policy = 'LAST'
        labels = {}
        
        i = 0
        while i < len(args):
            if args[i].upper() == 'RETENTION':
                if i + 1 >= len(args):
                    return "ERROR: RETENTION requires milliseconds value"
                try:
                    retention_ms = int(args[i + 1])
                    i += 2
                except ValueError:
                    return "ERROR: Invalid retention value"
            elif args[i].upper() == 'DUPLICATE_POLICY':
                if i + 1 >= len(args):
                    return "ERROR: DUPLICATE_POLICY requires policy name"
                duplicate_policy = args[i + 1].upper()
                if duplicate_policy not in ['BLOCK', 'FIRST', 'LAST']:
                    return "ERROR: Invalid duplicate policy"
                i += 2
            elif args[i].upper() == 'LABELS':
                i += 1
                while i < len(args) - 1:
                    labels[args[i]] = args[i + 1]
                    i += 2
            else:
                i += 1
        
        try:
            success = self.db.timeseries.create(
                key, retention_ms, duplicate_policy, labels)
            return "OK" if success else "ERROR: Key exists"
        except ValueError as e:
            return f"ERROR: {str(e)}"

    def ts_add_command(self, client_id, key, *args):
        """Add a sample. Format: TS.ADD key timestamp value"""
        if len(args) != 2:
            return "ERROR: Wrong number of arguments for TS.ADD"
        
        try:
            timestamp = '*' if args[0] == '*' else int(args[0])
            if timestamp == '*':
                timestamp = int(time.time() * 1000)  # Current time in milliseconds
            
            value = float(args[1])
            success = self.db.timeseries.add(key, timestamp, value)
            return str(timestamp) if success else "ERROR: Failed to add sample"
        except ValueError:
            return "ERROR: Invalid timestamp or value"

    def ts_get_command(self, client_id, key, *args):
        """Get latest sample or sample at timestamp. Format: TS.GET key [timestamp]"""
        timestamp = None
        if args:
            try:
                timestamp = int(args[0])
            except ValueError:
                return "ERROR: Invalid timestamp"
        
        result = self.db.timeseries.get(key, timestamp)
        if result:
            ts, val = result
            return [str(ts), str(val)]
        return "(nil)"

    def ts_range_command(self, client_id, key, *args):
        """Get range of samples. Format: TS.RANGE key fromTimestamp toTimestamp 
           [AGGREGATION aggregationType bucketSizeMs]"""
        if len(args) < 2:
            return "ERROR: Wrong number of arguments for TS.RANGE"
            
        try:
            from_ts = int(args[0])
            to_ts = int(args[1])
            agg_type = None
            bucket_size = None
            
            if len(args) > 2:
                if args[2].upper() != "AGGREGATION":
                    return "ERROR: Expected AGGREGATION keyword"
                if len(args) < 5:
                    return "ERROR: AGGREGATION requires type and bucket size"
                agg_type = args[3]
                bucket_size = int(args[4])
            
            result = self.db.timeseries.range(key, from_ts, to_ts, agg_type, bucket_size)
            if not result:
                return "(empty list)"
                
            # Format result as nested arrays: [[ts1, val1], [ts2, val2], ...]
            formatted = []
            for ts, val in result:
                # Each entry is a list containing timestamp and value
                formatted.append([str(ts), str(val)])
            return formatted
            
        except ValueError as e:
            return f"ERROR: {str(e)}"
