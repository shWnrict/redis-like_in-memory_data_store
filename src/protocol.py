class RESPProtocol:
    @staticmethod
    def parse_request(data):
        """
        Parses incoming RESP-formatted requests.
        For now, it supports only bulk strings and simple strings.
        """
        if data.startswith("+"):  # Simple String
            return data[1:].strip()
        elif data.startswith("$"):  # Bulk String
            _, value = data.split("\r\n", 1)
            return value.strip()
        else:
            raise ValueError("Unsupported RESP format")

    @staticmethod
    def format_response(response):
        """
        Formats the response as a RESP simple string.
        """
        return f"+{response}\r\n"