import asyncio
import json
import logging
from typing import Callable
import websockets
from websockets.legacy.client import WebSocketClientProtocol
from dataclasses import dataclass, field
from src.database import Database

wh_logger = logging.getLogger("websocket_handler")
wh_logger.setLevel(logging.INFO)
if not wh_logger.hasHandlers():
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    wh_logger.addHandler(handler)

whp_logger = logging.getLogger("websocket_handler.ping")
whp_logger.setLevel(logging.INFO)
if not whp_logger.hasHandlers():
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    whp_logger.addHandler(handler)

@dataclass
class ManifoldAPIWebsocketHandler:
    db: Database
    uri: str = "wss://api.manifold.markets/ws"
    connection: WebSocketClientProtocol = field(init=False, repr=False)
    txid: int = 0
    txid_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    running: bool = field(default=False, init=False, repr=False)

    async def connect(self):
        self.connection = await websockets.connect(self.uri) # type: ignore
        if not self.connection.open:
            raise ConnectionError("Failed to open WebSocket connection.")

    async def disconnect(self):
        if self.connection and self.connection.open:
            await self.connection.close()
            wh_logger.info("Disconnected from Manifold API WebSocket.")
        else:
            wh_logger.warning("WebSocket connection was not open.")

    async def send(self, message: Callable[[int], str]):
        if not self.connection.open:
            raise ConnectionError("WebSocket connection is not open.")
        async with self.txid_lock:
            self.txid += 1
            message_ = message(self.txid)
        await self.connection.send(message_)

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
        if self.running:
            wh_logger.warning("WebSocket handler is already running.")
            return
        self.running = True
        await self.connect()
        wh_logger.info("Connected to Manifold API WebSocket at %s. Subscribing to messages...", self.uri)

        await self.send(
            lambda txid: f'''
            {{
                "type": "subscribe",
                "txid": {txid},
                "topics": ["global/updated-contract"]
            }}
            '''
        )
        wh_logger.info("Subscription message sent. Waiting for response...")
        message = await self.receive_with_timeout(5)
        if message is None:
            wh_logger.error("No response received within timeout period.")
            await self.disconnect()
            raise ConnectionError("No response received from WebSocket after subscription.")
        wh_logger.info("Starting to listen for messages...")

        ping_task = asyncio.create_task(self.ping())
        handler_task = asyncio.create_task(asyncio.sleep(0)) # Placeholder for handler task
        while True:
            try:
                message = await self.receive()
                wh_logger.info("Received message: %s", message)
                handler_task = asyncio.create_task(self.handle_message(message))
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
        ping_task.cancel()
        handler_task.cancel()
        wh_logger.info("Stopping WebSocket handler.")
        self.running = False

    async def ping(self):
        while True:
            if not self.running:
                whp_logger.info("WebSocket handler is not running, stopping ping task.")
                return
            if not self.connection.open:
                whp_logger.warning("WebSocket connection is not open, skipping ping for now.")
                await asyncio.sleep(30)
                continue
            try:
                await self.send(lambda txid: f'{{"type": "ping", "txid": {txid}}}')
                whp_logger.info("Ping sent to WebSocket server.")
            except websockets.ConnectionClosed:
                whp_logger.error("WebSocket connection closed during ping.")
                await asyncio.sleep(30)
                continue
            except Exception as e:
                whp_logger.error("Error sending ping: %s", e)
            await asyncio.sleep(30)

    async def handle_message(self, message: websockets.Data):
        try:
            data = json.loads(message)
        except json.JSONDecodeError as e:
            wh_logger.error("Failed to decode JSON message: %s", e)
            return
        wh_logger.info("Handling message: %s", data)
        if data.get("type") == "broadcast":
            if data.get("topic") == "global/updated-contract":
                try:
                    payload = data["data"]
                except KeyError as e:
                    wh_logger.error("Unexpected; missing 'data' key in message: %s", e)
                    return
                title = payload.get("question")
                probability = payload.get("probability")
                idd = payload.get("id")
                if idd is None:
                    wh_logger.error("Unexpected; missing 'id' in payload: %s", payload)
                    return
                market = self.db.get_market(idd)
                if market is None:
                    wh_logger.info("Market with id '%s' not found in database.", idd)
                    return
                if title is not None:
                    market.title = title
                if probability is not None:
                    market.probability = probability
                self.db.update_market(idd, market)
                wh_logger.info("Updated market %s: %s", idd, market)
                return
            wh_logger.warning("Received broadcast message with unexpected topic: %s", data.get("topic"))
