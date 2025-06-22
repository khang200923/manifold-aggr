import asyncio
import logging
import websockets
from websockets.legacy.client import WebSocketClientProtocol
from dataclasses import dataclass, field

wh_logger = logging.getLogger("websocket_handler")
wh_logger.setLevel(logging.INFO)
if not wh_logger.hasHandlers():
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    wh_logger.addHandler(handler)

@dataclass
class ManifoldAPIWebsocketHandler:
    uri: str = "wss://api.manifold.markets/ws"
    connection: WebSocketClientProtocol = field(init=False, repr=False)

    async def connect(self):
        self.connection = await websockets.connect(self.uri) # type: ignore
        if not self.connection.open:
            raise ConnectionError("Failed to open WebSocket connection.")
        wh_logger.info("Connected to Manifold API WebSocket at %s", self.uri)

    async def disconnect(self):
        if self.connection and self.connection.open:
            await self.connection.close()
            wh_logger.info("Disconnected from Manifold API WebSocket.")
        else:
            wh_logger.warning("WebSocket connection was not open.")

    async def send(self, message: str):
        if not self.connection.open:
            raise ConnectionError("WebSocket connection is not open.")
        await self.connection.send(message)

    async def receive(self) -> websockets.Data:
        if not self.connection.open:
            raise ConnectionError("WebSocket connection is not open.")
        message = await self.connection.recv()
        return message

    async def receive_with_timeout(self, timeout: float) -> websockets.Data | None:
        if not self.connection.open:
            raise ConnectionError("WebSocket connection is not open.")
        try:
            message = await asyncio.wait_for(self.connection.recv(), timeout)
            return message
        except asyncio.TimeoutError:
            return None

    async def run(self):
        await self.connect()
        wh_logger.info("WebSocket connection established. Subscribing to messages...")

        await self.send(
            '''
            {
                "type": "subscribe",
                "txid": 69420,
                "topics": ["global/updated-contract"]
            }
            '''
        )
        message = await self.receive_with_timeout(5)
        if message is None:
            wh_logger.error("No response received within timeout period.")
            await self.disconnect()
            raise ConnectionError("No response received from WebSocket after subscription.")
        wh_logger.info("Starting to listen for messages...")

        while True:
            try:
                message = await self.receive()
                wh_logger.info("Received message: %s", message)
                asyncio.create_task(self.handle_message(message))
            except (websockets.ConnectionClosed, ConnectionError) as e:
                wh_logger.error("WebSocket connection error: %s", e)
                success = False
                for _ in range(5):  # Retry 5 times
                    try:
                        await self.connect()
                        wh_logger.info("Reconnected to WebSocket.")
                        success = True
                        break
                    except ConnectionError as reconnect_error:
                        wh_logger.error("Reconnection attempt failed: %s", reconnect_error)
                        await asyncio.sleep(2)
                if not success:
                    wh_logger.error("Failed to reconnect after multiple attempts. Exiting.")
                    break
            except KeyboardInterrupt:
                wh_logger.info("Keyboard interrupt received. Disconnecting...")
                await self.disconnect()
                break
            except Exception as e:
                wh_logger.error("Error receiving message: %s", e)
                await asyncio.sleep(1)

    async def handle_message(self, message: websockets.Data):
        ... # Nothing here yet
        # TODO: Implement message handling logic
