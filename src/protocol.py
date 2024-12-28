# Command parsing and response formatting

def parse_resp(data):
    """Parse RESP data into command and arguments."""
    lines = data.split("\r\n")
    if lines[0][0] == '*':
        num_args = int(lines[0][1:])
        args = []
        for i in range(num_args):
            arg_len = int(lines[2 * i + 1][1:])
            arg = lines[2 * i + 2]
            args.append(arg)
        return args
    return []

def format_resp(response):
    """Format response data into RESP format."""
    if isinstance(response, list):
        resp = f"*{len(response)}\r\n" + "".join([f"${len(str(item))}\r\n{item}\r\n" for item in response])
    elif isinstance(response, str):
        resp = f"+{response}\r\n"
    elif isinstance(response, int):
        resp = f":{response}\r\n"
    else:
        resp = "$-1\r\n"
    return resp