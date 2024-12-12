def test_server_initialization():
    from src.server import RedisServer
    server = RedisServer()
    assert server.host == 'localhost'
    assert server.port == 6379