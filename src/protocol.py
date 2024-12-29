class RESPError(Exception):
    pass

def parse_resp(data):
    """Parse RESP data into Python objects."""
    if not data:
        return None

    # Handle raw command input (not RESP formatted)
    if not data.startswith(('*', '+', '-', ':', '$')):
        return data.strip().split()
        
    try:
        parts = data.split('\r\n')
        if parts[0].startswith('*'):
            num_args = int(parts[0][1:])
            args = []
            i = 1
            for _ in range(num_args):
                if parts[i].startswith('$'):
                    length = int(parts[i][1:])
                    if length >= 0:
                        args.append(parts[i + 1])
                    i += 2
            return args if args else None
        elif parts[0].startswith('$'):
            length = int(parts[0][1:])
            return [parts[1]] if length >= 0 else None
        elif parts[0].startswith('+'):
            return [parts[0][1:]]
        elif parts[0].startswith(':'):
            return [int(parts[0][1:])]
        else:
            return None
    except (IndexError, ValueError) as e:
        print(f"Error parsing RESP: {e}")
        return data.strip().split()

def format_resp(data):
    """Format Python objects into RESP."""
    if data is None:
        return "+OK\r\n"  # Return OK for successful operations with no return value
    elif isinstance(data, str):
        if data.startswith("ERROR"):
            return f"-{data[6:]}\r\n"  # Convert ERROR: prefix to error response
        return f"+{data}\r\n"
    elif isinstance(data, int):
        return f":{data}\r\n"
    elif isinstance(data, list):
        if not data:
            return "*0\r\n"
        parts = [f"*{len(data)}\r\n"]
        for item in data:
            if isinstance(item, str):
                parts.append(f"${len(item)}\r\n{item}\r\n")
            else:
                parts.append(format_resp(item))
        return "".join(parts)
    else:
        data_str = str(data)
        return f"${len(data_str)}\r\n{data_str}\r\n"

def format_pubsub_message(message_type, channel, data=None):
    """Format Pub/Sub messages according to Redis protocol."""
    if message_type == "subscribe":
        return (
            "*3\r\n"
            "$9\r\n"
            "subscribe\r\n"
            f"${len(channel)}\r\n"
            f"{channel}\r\n"
            ":1\r\n"
        )
    elif message_type == "message":
        return (
            "*3\r\n"
            "$7\r\n"
            "message\r\n"
            f"${len(channel)}\r\n"
            f"{channel}\r\n"
            f"${len(data)}\r\n"
            f"{data}\r\n"
        )
    elif message_type == "unsubscribe":
        return (
            "*3\r\n"
            "$11\r\n"
            "unsubscribe\r\n"
            f"${len(channel)}\r\n"
            f"{channel}\r\n"
            ":0\r\n"
        )
    return None

def parse_next(data):
    """Helper to parse next element in array."""
    if not data:
        return None, 0
    
    first_byte = data[0]
    end = data.find('\r\n')
    if end == -1:
        return None, 0
        
    if first_byte == '$':
        length = int(data[1:end])
        if length == -1:
            return None, end + 2
        start = end + 2
        return data[start:start + length], start + length + 2
    
    return data[1:end], end + 2