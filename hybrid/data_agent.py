"""
Data Agent - Live market data from CCXT
"""
import asyncio
import ccxt.async_support as ccxt
from hybrid.agent_base import BaseAgent
from hybrid.messaging import MessageBus, Channel


class DataAgent(BaseAgent):
    def __init__(self, bus: MessageBus, symbols: list[str], exchange_id: str = "kraken"):
        super().__init__("data_agent", bus)
        self.symbols = symbols
        self.exchange_id = exchange_id
        self.exchange = None
        self.tasks = []
    
    async def handle_message(self, payload: dict):
        pass  # No incoming messages for data agent
    
    async def start(self):
        self.running = True
        self.exchange = getattr(ccxt, self.exchange_id)({
            'enableRateLimit': True,
            'timeout': 30000,  # 30 second timeout
        })
        for symbol in self.symbols:
            task = asyncio.create_task(self._stream_ticker(symbol))
            self.tasks.append(task)
        # removed blocking wait
    
    async def stop(self):
        self.running = False
        for task in self.tasks:
            task.cancel()
        if self.exchange:
            await self.exchange.close()
    
    async def _stream_ticker(self, symbol: str):
        while self.running:
            try:
                # Add timeout to fetch_ticker to prevent hanging
                ticker = await asyncio.wait_for(
                    self.exchange.fetch_ticker(symbol),
                    timeout=10.0  # 10 second timeout per request
                )
                self.bus.publish(Channel.MARKET_DATA, {
                    "symbol": symbol,
                    "price": ticker['last'],
                    "bid": ticker['bid'],
                    "ask": ticker['ask'],
                    "volume": ticker['baseVolume'],
                    "timestamp": ticker['timestamp']
                }, self.name)
            except asyncio.TimeoutError:
                print(f"[DataAgent] Timeout fetching ticker for {symbol}")
            except Exception as e:
                print(f"[DataAgent] Error: {e}")
            await asyncio.sleep(1)
