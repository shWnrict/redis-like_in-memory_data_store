from .connection import Connection
from .pipeline import Pipeline
from src.protocol import RESPProtocol
from .error_handling import RedisError, ConnectionError, ResponseError, InvalidResponse

class RedisClient:
    def __init__(self, host='localhost', port=6379):
        self.connection = Connection(host, port)
        self.pipeline = Pipeline(self.connection)

    def execute(self, *args):
        try:
            self.connection.send_command(*args)
            return self.connection.read_response()
        except (ConnectionError, ResponseError, InvalidResponse) as e:
            raise RedisError(str(e))

    def pipeline_commands(self):
        return self.pipeline

    def close(self):
        self.connection.disconnect()

    def set(self, key, value):
        return self.execute('SET', key, value)

    def get(self, key):
        return self.execute('GET', key)

    def set_multiple(self, key_value_pairs):
        pipe = self.pipeline_commands()
        for key, value in key_value_pairs.items():
            pipe.set(key, value)
        return pipe.execute()

if __name__ == "__main__":
    client = RedisClient()
    try:
        client.set('foo', 'bar')
        print(client.get('foo'))

        responses = client.set_multiple({'key1': 'value1', 'key2': 'value2'})
        print(responses)
        print(client.get('key1'))
        print(client.get('key2'))
    except RedisError as e:
        print(f"Redis error: {e}")
    finally:
        client.close()