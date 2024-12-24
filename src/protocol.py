class RESPProtocol:
    @staticmethod
    def parse_request(data):
        """
        Parses incoming RESP-formatted requests.
        Supports simple strings, bulk strings, errors, integers, and arrays.
        """
        if data.startswith("+"):  # Simple String
            return data[1:].strip()
        elif data.startswith("$"):  # Bulk String
            _, value = data.split("\r\n", 1)
            return value.strip()
        elif data.startswith("-"):  # Error
            return {"error": data[1:].strip()}
        elif data.startswith(":"):  # Integer
            return int(data[1:].strip())
        elif data.startswith("*"):  # Array
            parts = data.split("\r\n")
            length = int(parts[0][1:])  # Number of elements
            elements = parts[1:]
            result = []
            for i in range(length):
                if elements[i].startswith("$"):
                    result.append(elements[i+1])
            return result
        else:
            raise ValueError("Unsupported RESP format")

    @staticmethod
    def format_response(response):
        """
        Formats the response as a RESP simple string, bulk string, error, integer, or array.
        """
        if isinstance(response, str):
            return f"+{response}\r\n"  # Simple String
        elif isinstance(response, int):
            return f":{response}\r\n"  # Integer
        elif isinstance(response, list):
            array_response = f"*{len(response)}\r\n"
            for item in response:
                array_response += f"${len(item)}\r\n{item}\r\n"
            return array_response  # Array
        elif isinstance(response, dict) and "error" in response:
            return f"-{response['error']}\r\n"  # Error
        else:
            return f"${len(response)}\r\n{response}\r\n"  # Bulk String
