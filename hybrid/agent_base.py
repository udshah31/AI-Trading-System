"""
Base Agent Class for async agent framework
"""
from abc import ABC, abstractmethod
from typing import Dict, Any

from hybrid.messaging import MessageBus, Channel


class BaseAgent(ABC):
    def __init__(self, name: str, bus: MessageBus):
        self.name = name
        self.bus = bus
        self.running = False
    
    @abstractmethod
    async def handle_message(self, payload: Dict[str, Any]): ...
    
    async def start(self):
        self.running = True
        self.bus.subscribe(Channel.SIGNALS, self.handle_message)
        print(f"[{self.name}] Started")
    
    async def stop(self):
        self.running = False
        print(f"[{self.name}] Stopped")
