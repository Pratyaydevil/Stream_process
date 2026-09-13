import unittest

from fastapi.testclient import TestClient

import api.app.main as app_module


class WebSocketRouteTest(unittest.TestCase):
    def test_websocket_route_exists(self):
        app_module.query_database = lambda *args, **kwargs: []

        with TestClient(app_module.app) as client:
            with client.websocket_connect("/ws") as websocket:
                message = websocket.receive_json()
                self.assertEqual(message["type"], "connected")


if __name__ == "__main__":
    unittest.main()
