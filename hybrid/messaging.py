"""
Redis Pub/Sub Message Bus for inter-agent communication
"""
import asyncio
import json
import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Dict, List, Any, Optional

import redis.asyncio as redis


class Channel(Enum):
    MARKET_DATA = "market:data"
    SIGNALS = "signals"
    ORDERS = "orders"
    RISK_ALERTS = "risk:alerts"
    HEARTBEAT = "system:heartbeat"


@dataclass
class Message:
    channel: str
    payload: dict
    timestamp: float
    source: str


class MessageBus:
    def __init__(self, url: str = "redis://localhost:6379"):
        self.client = redis.from_url(url, decode_responses=True)
        self.subscribers: Dict[str, List[Callable]] = {}
        self._pubsub = None
        self._listen_task = None
        self._pending_tasks: set = set()
    
    async def start(self):
        """Start listening for messages on ALL known channels.
        
        Handlers can register later via subscribe() — dispatch is dynamic.
        """
        self._pubsub = self.client.pubsub()
        await self._pubsub.subscribe(*[c.value for c in Channel])
        self._listen_task = asyncio.create_task(self._listen())
    
    async def stop(self):
        if self._listen_task:
            self._listen_task.cancel()
        if self._pubsub:
            await self._pubsub.close()
        await self.client.close()
    
    def publish(self, channel: Channel, payload: dict, source: str):
        msg = Message(
            channel=channel.value,
            payload=payload,
            timestamp=time.time(),
            source=source
        )
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        task = loop.create_task(self._publish(channel.value, json.dumps(msg.__dict__)))
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def _publish(self, channel: str, data: str):
        try:
            await self.client.publish(channel, data)
        except Exception as e:
            print(f"[MessageBus] publish error on {channel}: {e}")
    
    def subscribe(self, channel: Channel, handler: Callable):
        if channel.value not in self.subscribers:
            self.subscribers[channel.value] = []
        self.subscribers[channel.value].append(handler)
    
    async def _listen(self):
        async for msg in self._pubsub.listen():
            if msg['type'] == 'message':
                try:
                    data = json.loads(msg['data'])
                    channel = data['channel']
                    for handler in self.subscribers.get(channel, []):
                        await handler(data['payload'])
                except Exception as e:
                    print(f"Message handling error: {e}")
