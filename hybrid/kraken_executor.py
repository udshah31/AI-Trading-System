"""
Kraken Pro Executor
===================
REST + WebSocket client for Kraken Pro (Spot & Futures).
Replaces Alpaca for live crypto execution.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, List, Any, Callable
from urllib.parse import urlencode

import aiohttp
import websockets


class KrakenEnvironment(Enum):
    SPOT = "spot"
    FUTURES = "futures"
    SPOT_DEMO = "spot_demo"      # Not available on Kraken
    FUTURES_DEMO = "futures_demo"  # futures.kraken.com (demo)


@dataclass
class KrakenConfig:
    api_key: str
    api_secret: str              # Base64 encoded
    passphrase: str = ""         # Optional, for futures
    environment: KrakenEnvironment = KrakenEnvironment.SPOT
    
    # Rate limits
    rest_rate_limit: int = 15    # requests/second
    ws_ping_interval: int = 30
    
    # Endpoints
    @property
    def rest_base_url(self) -> str:
        if self.environment == KrakenEnvironment.FUTURES:
            return "https://futures.kraken.com"
        return "https://api.kraken.com"
    
    @property
    def ws_url(self) -> str:
        if self.environment == KrakenEnvironment.FUTURES:
            return "wss://futures.kraken.com/ws/v1"
        return "wss://ws.kraken.com"


@dataclass
class OrderRequest:
    symbol: str                  # e.g., "BTC/USD", "XBT/USD" (Kraken format)
    side: str                    # "buy" or "sell"
    order_type: str              # "market", "limit", "stop-loss", "take-profit"
    volume: Decimal              # Base currency amount
    price: Optional[Decimal] = None
    leverage: Optional[int] = None  # Futures only
    reduce_only: bool = False
    client_order_id: Optional[str] = None
    validate_only: bool = False


@dataclass
class OrderResult:
    success: bool
    order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    symbol: str = ""
    side: str = ""
    order_type: str = ""
    volume: Decimal = Decimal("0")
    price: Optional[Decimal] = None
    filled_volume: Decimal = Decimal("0")
    avg_fill_price: Optional[Decimal] = None
    status: str = ""             # "open", "closed", "canceled", "rejected"
    fee: Decimal = Decimal("0")
    fee_currency: str = ""
    timestamp: float = 0
    error: Optional[str] = None
    raw_response: Dict = field(default_factory=dict)


@dataclass
class Balance:
    asset: str
    free: Decimal
    used: Decimal
    total: Decimal


@dataclass
class Ticker:
    symbol: str
    bid: Decimal
    ask: Decimal
    last: Decimal
    volume_24h: Decimal
    high_24h: Decimal
    low_24h: Decimal
    change_24h_pct: float
    timestamp: float


class KrakenAuth:
    """Handles Kraken API authentication"""
    
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = base64.b64decode(api_secret)
    
    def get_headers(self, uri_path: str, data: Dict) -> Dict[str, str]:
        """Generate authentication headers for REST API"""
        nonce = str(int(time.time() * 1000))
        postdata = urlencode(data)
        
        # Create signature
        message = uri_path.encode() + hashlib.sha256(
            (nonce + postdata).encode()
        ).digest()
        
        signature = hmac.new(
            self.api_secret,
            message,
            hashlib.sha512
        ).digest()
        
        return {
            "API-Key": self.api_key,
            "API-Sign": base64.b64encode(signature).decode(),
            "Content-Type": "application/x-www-form-urlencoded"
        }
    
    def get_futures_headers(self, method: str, path: str, body: str = "") -> Dict[str, str]:
        """Generate authentication headers for Futures API"""
        nonce = str(int(time.time() * 1000))
        message = f"{method}{path}{nonce}{body}".encode()
        
        signature = hmac.new(
            self.api_secret,
            message,
            hashlib.sha512
        ).digest()
        
        return {
            "APIKey": self.api_key,
            "Nonce": nonce,
            "Authent": base64.b64encode(signature).decode(),
            "Content-Type": "application/json"
        }


class KrakenRestClient:
    """REST API client for Kraken Spot & Futures"""
    
    def __init__(self, config: KrakenConfig):
        self.config = config
        self.auth = KrakenAuth(config.api_key, config.api_secret)
        self.session: Optional[aiohttp.ClientSession] = None
        self._rate_limiter = asyncio.Semaphore(config.rest_rate_limit)
        self._last_request = 0
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
    
    async def _rate_limit(self):
        async with self._rate_limiter:
            now = time.time()
            elapsed = now - self._last_request
            if elapsed < 1.0 / self.config.rest_rate_limit:
                await asyncio.sleep(1.0 / self.config.rest_rate_limit - elapsed)
            self._last_request = time.time()
    
    async def _request(self, method: str, endpoint: str, params: Dict = None, 
                       auth: bool = False, futures: bool = False) -> Dict:
        await self._rate_limit()
        
        if not self.session:
            raise RuntimeError("Session not initialized")
        
        url = f"{self.config.rest_base_url}{endpoint}"
        
        if futures:
            # Futures API uses JSON body
            body = json.dumps(params or {})
            headers = self.auth.get_futures_headers(method, endpoint, body)
            async with self.session.request(method, url, data=body, headers=headers) as resp:
                return await resp.json()
        else:
            # Spot API uses form data
            data = params or {}
            data["nonce"] = str(int(time.time() * 1000))
            
            if auth:
                headers = self.auth.get_headers(endpoint, data)
            else:
                headers = {"Content-Type": "application/x-www-form-urlencoded"}
            
            async with self.session.request(method, url, data=data, headers=headers) as resp:
                result = await resp.json()
                if result.get("error"):
                    raise Exception(f"Kraken API error: {result['error']}")
                return result.get("result", result)
    
    # ============ PUBLIC ENDPOINTS ============
    
    async def get_server_time(self) -> Dict:
        return await self._request("GET", "/0/public/Time")
    
    async def get_asset_pairs(self) -> Dict:
        return await self._request("GET", "/0/public/AssetPairs")
    
    async def get_ticker(self, pairs: List[str]) -> Dict:
        pair_str = ",".join(pairs)
        return await self._request("GET", "/0/public/Ticker", {"pair": pair_str})
    
    async def get_ohlc(self, pair: str, interval: int = 1, since: int = None) -> Dict:
        params = {"pair": pair, "interval": interval}
        if since:
            params["since"] = since
        return await self._request("GET", "/0/public/OHLC", params)
    
    async def get_order_book(self, pair: str, count: int = 100) -> Dict:
        return await self._request("GET", "/0/public/Depth", {"pair": pair, "count": count})
    
    async def get_recent_trades(self, pair: str, since: int = None) -> Dict:
        params = {"pair": pair}
        if since:
            params["since"] = since
        return await self._request("GET", "/0/public/Trades", params)
    
    async def get_futures_instruments(self) -> Dict:
        return await self._request("GET", "/derivatives/api/v3/instruments", futures=True)
    
    async def get_futures_tickers(self) -> Dict:
        return await self._request("GET", "/derivatives/api/v3/tickers", futures=True)
    
    # ============ PRIVATE ENDPOINTS ============
    
    async def get_account_balance(self) -> Dict[str, Decimal]:
        """Get spot account balances"""
        result = await self._request("POST", "/0/private/Balance", {}, auth=True)
        return {k: Decimal(v) for k, v in result.items()}
    
    async def get_trade_balance(self, asset: str = "ZUSD") -> Dict:
        return await self._request("POST", "/0/private/TradeBalance", 
                                   {"asset": asset}, auth=True)
    
    async def get_open_orders(self) -> Dict:
        return await self._request("POST", "/0/private/OpenOrders", {}, auth=True)
    
    async def get_closed_orders(self, start: int = None, end: int = None) -> Dict:
        params = {}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        return await self._request("POST", "/0/private/ClosedOrders", params, auth=True)
    
    async def query_orders(self, txids: List[str]) -> Dict:
        return await self._request("POST", "/0/private/QueryOrders", 
                                   {"txid": ",".join(txids)}, auth=True)
    
    async def get_trades_history(self, start: int = None, end: int = None) -> Dict:
        params = {}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        return await self._request("POST", "/0/private/TradesHistory", params, auth=True)
    
    async def get_futures_account(self) -> Dict:
        return await self._request("POST", "/derivatives/api/v3/accounts", {}, 
                                   auth=True, futures=True)
    
    async def get_futures_positions(self) -> Dict:
        return await self._request("POST", "/derivatives/api/v3/openpositions", {},
                                   auth=True, futures=True)
    
    async def get_futures_fills(self, last_time: str = None) -> Dict:
        params = {}
        if last_time:
            params["lastTime"] = last_time
        return await self._request("POST", "/derivatives/api/v3/fills", params,
                                   auth=True, futures=True)
    
    # ============ ORDER MANAGEMENT ============
    
    async def add_order(self, order: OrderRequest) -> OrderResult:
        """Place a new order (spot)"""
        # Convert symbol to Kraken format (XBT instead of BTC)
        pair = self._normalize_symbol(order.symbol)
        
        data = {
            "pair": pair,
            "type": order.side,
            "ordertype": order.order_type,
            "volume": str(order.volume),
        }
        
        if order.price:
            data["price"] = str(order.price)
        
        if order.client_order_id:
            data["userref"] = order.client_order_id
        
        if order.validate_only:
            data["validate"] = "true"
        
        # Add leverage for margin orders (spot margin)
        if order.leverage:
            data["leverage"] = str(order.leverage)
        
        result = await self._request("POST", "/0/private/AddOrder", data, auth=True)
        
        return OrderResult(
            success=len(result.get("txid", [])) > 0,
            order_id=result.get("txid", [None])[0],
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            volume=order.volume,
            price=order.price,
            status="open" if result.get("txid") else "rejected",
            raw_response=result
        )
    
    async def add_futures_order(self, order: OrderRequest) -> OrderResult:
        """Place a futures order"""
        symbol = self._normalize_futures_symbol(order.symbol)
        
        body = {
            "orderType": "mkt" if order.order_type == "market" else "lmt",
            "symbol": symbol,
            "side": order.side.capitalize(),
            "size": str(order.volume),
        }
        
        if order.order_type == "limit" and order.price:
            body["limitPrice"] = str(order.price)
        
        if order.reduce_only:
            body["reduceOnly"] = True
        
        if order.client_order_id:
            body["cliOrdId"] = order.client_order_id
        
        result = await self._request("POST", "/derivatives/api/v3/sendorder", body,
                                     auth=True, futures=True)
        
        return OrderResult(
            success=result.get("result") == "success",
            order_id=result.get("order_id"),
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            side=order.side,
            order_type=order.order_type,
            volume=order.volume,
            price=order.price,
            status="open" if result.get("result") == "success" else "rejected",
            raw_response=result
        )
    
    async def cancel_order(self, order_id: str, futures: bool = False) -> OrderResult:
        """Cancel an order"""
        if futures:
            body = {"order_id": order_id}
            result = await self._request("POST", "/derivatives/api/v3/cancelorder", body,
                                         auth=True, futures=True)
        else:
            result = await self._request("POST", "/0/private/CancelOrder",
                                         {"txid": order_id}, auth=True)
        
        return OrderResult(
            success=result.get("count", 0) > 0 or result.get("result") == "success",
            order_id=order_id,
            status="canceled",
            raw_response=result
        )
    
    async def cancel_all_orders(self, futures: bool = False) -> Dict:
        """Cancel all open orders"""
        if futures:
            return await self._request("POST", "/derivatives/api/v3/cancelallorders",
                                       {}, auth=True, futures=True)
        else:
            return await self._request("POST", "/0/private/CancelAllOrders", {},
                                       auth=True)
    
    # ============ UTILITIES ============
    
    def _normalize_symbol(self, symbol: str) -> str:
        """Convert standard symbol to Kraken format"""
        # BTC -> XBT, USD -> ZUSD, etc.
        replacements = {
            "BTC": "XBT",
            "BTC/USD": "XBT/USD",
            "BTC/USDT": "XBT/USDT",
            "ETH/USD": "ETH/USD",
            "SOL/USD": "SOL/USD",
            "USD": "ZUSD",
            "USDT": "USDT",
        }
        for old, new in replacements.items():
            symbol = symbol.replace(old, new)
        return symbol
    
    def _normalize_futures_symbol(self, symbol: str) -> str:
        """Convert to futures symbol format (PI_XBTUSD)"""
        if symbol.startswith("PI_"):
            return symbol
        
        # Convert BTC/USD -> PI_XBTUSD
        symbol = symbol.replace("BTC", "XBT").replace("/", "")
        return f"PI_{symbol}"


class KrakenWebSocket:
    """WebSocket client for real-time market data"""
    
    def __init__(self, config: KrakenConfig):
        self.config = config
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.subscriptions: Dict[str, Dict] = {}
        self.callbacks: Dict[str, List[Callable]] = {}
        self.running = False
        self._reconnect_task: Optional[asyncio.Task] = None
    
    def subscribe_ticker(self, symbols: List[str], callback: Callable):
        """Subscribe to ticker updates"""
        self._subscribe("ticker", {"pair": symbols}, callback)
    
    def subscribe_ohlc(self, symbols: List[str], interval: int, callback: Callable):
        """Subscribe to OHLC/candle updates"""
        self._subscribe("ohlc", {"pair": symbols, "interval": interval}, callback)
    
    def subscribe_book(self, symbols: List[str], depth: int, callback: Callable):
        """Subscribe to order book updates"""
        self._subscribe("book", {"pair": symbols, "depth": depth}, callback)
    
    def subscribe_trades(self, symbols: List[str], callback: Callable):
        """Subscribe to trade feed"""
        self._subscribe("trade", {"pair": symbols}, callback)
    
    def subscribe_own_trades(self, callback: Callable):
        """Subscribe to own trades (requires auth)"""
        self._subscribe("ownTrades", {}, callback, auth=True)
    
    def subscribe_open_orders(self, callback: Callable):
        """Subscribe to open orders (requires auth)"""
        self._subscribe("openOrders", {}, callback, auth=True)
    
    def _subscribe(self, channel: str, params: Dict, callback: Callable, auth: bool = False):
        key = f"{channel}:{json.dumps(params, sort_keys=True)}"
        if key not in self.subscriptions:
            self.subscriptions[key] = {"channel": channel, "params": params, "auth": auth}
        if channel not in self.callbacks:
            self.callbacks[channel] = []
        self.callbacks[channel].append(callback)
    
    async def connect(self):
        """Connect and subscribe to all channels"""
        self.running = True
        
        while self.running:
            try:
                async with websockets.connect(self.config.ws_url) as ws:
                    self.ws = ws
                    print(f"[Kraken WS] Connected to {self.config.ws_url}")
                    
                    # Subscribe to all channels
                    for sub in self.subscriptions.values():
                        await self._send_subscription(sub)
                    
                    # Message loop
                    async for message in ws:
                        await self._handle_message(json.loads(message))
                        
            except websockets.exceptions.ConnectionClosed:
                print("[Kraken WS] Connection closed, reconnecting...")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"[Kraken WS] Error: {e}, reconnecting in 5s...")
                await asyncio.sleep(5)
    
    async def _send_subscription(self, sub: Dict):
        msg = {"event": "subscribe", **sub}
        if sub.get("auth") and self.config.api_key:
            # Add auth for private channels
            msg["token"] = await self._get_ws_token()
        await self.ws.send(json.dumps(msg))
    
    async def _get_ws_token(self) -> str:
        """Get WebSocket authentication token"""
        # For spot: need to call GetWebSocketsToken endpoint
        # For futures: use API key directly
        # Simplified - would need proper implementation
        return ""
    
    async def _handle_message(self, msg: Dict):
        if "event" in msg:
            # System events
            if msg["event"] in ["systemStatus", "subscriptionStatus", "heartbeat"]:
                return
        
        # Channel data
        if isinstance(msg, list) and len(msg) >= 4:
            channel_name = msg[2]
            data = msg[1]
            
            for callback in self.callbacks.get(channel_name, []):
                try:
                    await callback(data, channel_name)
                except Exception as e:
                    print(f"[Kraken WS] Callback error: {e}")
    
    async def close(self):
        self.running = False
        if self.ws:
            await self.ws.close()


class KrakenExecutor:
    """
    High-level Kraken executor integrating REST + WebSocket.
    Compatible with the hybrid system's message bus.
    """
    
    def __init__(self, config: KrakenConfig):
        self.config = config
        self.rest: Optional[KrakenRestClient] = None
        self.ws: Optional[KrakenWebSocket] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._closed = False
        
        # Caches
        self._symbol_map: Dict[str, str] = {}  # standard -> kraken
        self._asset_map: Dict[str, str] = {}   # kraken -> standard
        self._balances: Dict[str, Balance] = {}
        self._tickers: Dict[str, Ticker] = {}
        self._open_orders: Dict[str, OrderResult] = {}
    
    async def initialize(self):
        """Initialize REST client, load symbols, start WebSocket"""
        self.rest = KrakenRestClient(self.config)
        await self.rest.__aenter__()
        
        # Load symbol mappings
        await self._load_symbols()
        
        # Start WebSocket
        self.ws = KrakenWebSocket(self.config)
        self._ws_task = asyncio.create_task(self.ws.connect())
        
        # Subscribe to public tickers for major pairs
        major_pairs = ["XBT/USD", "ETH/USD", "SOL/USD", "XBT/USDT"]
        self.ws.subscribe_ticker(major_pairs, self._on_ticker_update)
        
        print(f"[Kraken] Initialized ({self.config.environment.value})")
    
    async def close(self):
        if self._closed:
            return
        self._closed = True
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await asyncio.wait_for(
                    asyncio.shield(self._ws_task), timeout=3.0
                )
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass
        if self.ws:
            try:
                await asyncio.wait_for(self.ws.close(), timeout=3.0)
            except Exception:
                pass
        if self.rest:
            try:
                await asyncio.wait_for(
                    self.rest.__aexit__(None, None, None), timeout=5.0
                )
            except Exception:
                pass
    
    async def _load_symbols(self):
        """Load and cache symbol mappings"""
        if self.config.environment == KrakenEnvironment.FUTURES:
            result = await self.rest.get_futures_instruments()
            for inst in result.get("instruments", []):
                self._symbol_map[inst["symbol"]] = inst["symbol"]
        else:
            result = await self.rest.get_asset_pairs()
            for kraken_symbol, info in result.items():
                # Map standard -> kraken
                standard = info.get("altname", kraken_symbol).replace(".", "/")
                self._symbol_map[standard] = kraken_symbol
                self._asset_map[kraken_symbol] = standard
    
    def _to_kraken_symbol(self, symbol: str) -> str:
        return self._symbol_map.get(symbol, symbol)
    
    def _to_standard_symbol(self, kraken_symbol: str) -> str:
        return self._asset_map.get(kraken_symbol, kraken_symbol)
    
    # ============ MARKET DATA ============
    
    def _on_ticker_update(self, data: Dict, channel: str):
        """Handle ticker WebSocket updates"""
        for symbol, ticker_data in data.items():
            if isinstance(ticker_data, dict):
                try:
                    standard_symbol = self._to_standard_symbol(symbol)
                    self._tickers[standard_symbol] = Ticker(
                        symbol=standard_symbol,
                        bid=Decimal(str(ticker_data.get("b", [0])[0])),
                        ask=Decimal(str(ticker_data.get("a", [0])[0])),
                        last=Decimal(str(ticker_data.get("c", [0])[0])),
                        volume_24h=Decimal(str(ticker_data.get("v", [0])[1])),
                        high_24h=Decimal(str(ticker_data.get("h", [0])[1])),
                        low_24h=Decimal(str(ticker_data.get("l", [0])[1])),
                        change_24h_pct=float(ticker_data.get("p", [0])[1]),
                        timestamp=time.time()
                    )
                except Exception:
                    pass
    
    async def get_ticker(self, symbol: str) -> Optional[Ticker]:
        """Get current ticker (from cache or REST)"""
        standard = self._to_standard_symbol(symbol)
        if standard in self._tickers:
            return self._tickers[standard]
        
        # Fallback to REST
        kraken_symbol = self._to_kraken_symbol(symbol)
        result = await self.rest.get_ticker([kraken_symbol])
        
        if kraken_symbol in result:
            d = result[kraken_symbol]
            return Ticker(
                symbol=standard,
                bid=Decimal(d["b"][0]),
                ask=Decimal(d["a"][0]),
                last=Decimal(d["c"][0]),
                volume_24h=Decimal(d["v"][1]),
                high_24h=Decimal(d["h"][1]),
                low_24h=Decimal(d["l"][1]),
                change_24h_pct=float(d["p"][1]),
                timestamp=time.time()
            )
        return None
    
    async def get_orderbook(self, symbol: str, depth: int = 20) -> Dict:
        kraken_symbol = self._to_kraken_symbol(symbol)
        return await self.rest.get_order_book(kraken_symbol, depth)
    
    # ============ ACCOUNT ============
    
    async def get_balances(self) -> Dict[str, Balance]:
        """Get all account balances"""
        if self.config.environment == KrakenEnvironment.FUTURES:
            result = await self.rest.get_futures_account()
            for asset, data in result.get("accounts", {}).items():
                self._balances[asset] = Balance(
                    asset=asset,
                    free=Decimal(data.get("available", 0)),
                    used=Decimal(data.get("margin", 0)),
                    total=Decimal(data.get("balance", 0))
                )
        else:
            result = await self.rest.get_account_balance()
            for asset, total in result.items():
                standard = self._to_standard_symbol(asset)
                self._balances[standard] = Balance(
                    asset=standard,
                    free=total,  # Would need separate call for free/used
                    used=Decimal("0"),
                    total=total
                )
        return self._balances
    
    async def get_balance(self, asset: str) -> Optional[Balance]:
        balances = await self.get_balances()
        return balances.get(asset)
    
    # ============ TRADING ============
    
    async def place_order(self, order: OrderRequest) -> OrderResult:
        """Place an order"""
        if self.config.environment == KrakenEnvironment.FUTURES:
            result = await self.rest.add_futures_order(order)
        else:
            result = await self.rest.add_order(order)
        
        if result.success and result.order_id:
            self._open_orders[result.order_id] = result
        
        return result
    
    async def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel an order"""
        result = await self.rest.cancel_order(
            order_id, 
            futures=self.config.environment == KrakenEnvironment.FUTURES
        )
        
        if result.success and order_id in self._open_orders:
            self._open_orders[order_id].status = "canceled"
        
        return result
    
    async def cancel_all_orders(self) -> Dict:
        return await self.rest.cancel_all_orders(
            futures=self.config.environment == KrakenEnvironment.FUTURES
        )
    
    async def get_order_status(self, order_id: str) -> Optional[OrderResult]:
        if order_id in self._open_orders:
            return self._open_orders[order_id]
        
        # Query from API
        if self.config.environment == KrakenEnvironment.FUTURES:
            # Futures: query via open positions or fills
            pass
        else:
            result = await self.rest.query_orders([order_id])
            # Parse result...
        
        return None
    
    # ============ HIGH-LEVEL HELPERS ============
    
    async def market_buy(self, symbol: str, volume: Decimal) -> OrderResult:
        return await self.place_order(OrderRequest(
            symbol=symbol, side="buy", order_type="market", volume=volume
        ))
    
    async def market_sell(self, symbol: str, volume: Decimal) -> OrderResult:
        return await self.place_order(OrderRequest(
            symbol=symbol, side="sell", order_type="market", volume=volume
        ))
    
    async def limit_buy(self, symbol: str, volume: Decimal, price: Decimal) -> OrderResult:
        return await self.place_order(OrderRequest(
            symbol=symbol, side="buy", order_type="limit", volume=volume, price=price
        ))
    
    async def limit_sell(self, symbol: str, volume: Decimal, price: Decimal) -> OrderResult:
        return await self.place_order(OrderRequest(
            symbol=symbol, side="sell", order_type="limit", volume=volume, price=price
        ))
    
    async def buy_usd_amount(self, symbol: str, usd_amount: Decimal) -> OrderResult:
        """Buy $X worth of symbol at market"""
        ticker = await self.get_ticker(symbol)
        if not ticker:
            raise Exception(f"No ticker for {symbol}")
        
        volume = usd_amount / ticker.ask
        # Round to lot size (would need asset pair info)
        volume = volume.quantize(Decimal("0.00000001"))
        
        return await self.market_buy(symbol, volume)
    
    async def sell_all(self, symbol: str) -> OrderResult:
        """Sell entire position of symbol at market"""
        base_asset = symbol.split("/")[0].replace("BTC", "XBT")
        balance = await self.get_balance(base_asset)
        
        if not balance or balance.free <= 0:
            return OrderResult(success=False, error=f"No {base_asset} balance")
        
        return await self.market_sell(symbol, balance.free)
    
    # ============ FUTURES SPECIFIC ============
    
    async def get_futures_positions(self) -> List[Dict]:
        if self.config.environment != KrakenEnvironment.FUTURES:
            return []
        result = await self.rest.get_futures_positions()
        return result.get("openPositions", [])
    
    async def set_leverage(self, symbol: str, leverage: int) -> Dict:
        if self.config.environment != KrakenEnvironment.FUTURES:
            raise Exception("Not futures environment")
        futures_symbol = self._normalize_futures_symbol(symbol)
        return await self.rest._request("POST", "/derivatives/api/v3/setleverage",
                                        {"symbol": futures_symbol, "leverage": leverage},
                                        auth=True, futures=True)


# =============================================================================
# FACTORY & INTEGRATION
# =============================================================================

class KrakenExecutorFactory:
    """Factory for creating Kraken executors"""
    
    _instances: Dict[KrakenEnvironment, KrakenExecutor] = {}
    
    @classmethod
    async def get_executor(cls, env: KrakenEnvironment = KrakenEnvironment.SPOT, 
                           **kwargs) -> KrakenExecutor:
        if env not in cls._instances:
            config = KrakenConfig(
                api_key=kwargs.get("api_key") or os.getenv("KRAKEN_API_KEY"),
                api_secret=kwargs.get("api_secret") or os.getenv("KRAKEN_API_SECRET"),
                passphrase=kwargs.get("passphrase") or os.getenv("KRAKEN_PASSPHRASE", ""),
                environment=env
            )
            executor = KrakenExecutor(config)
            await executor.initialize()
            cls._instances[env] = executor
        return cls._instances[env]
    
    @classmethod
    async def close_all(cls):
        for exec in cls._instances.values():
            await exec.close()
        cls._instances.clear()


# =============================================================================
# INTEGRATION WITH HYBRID SYSTEM
# =============================================================================

class KrakenExecutionAgent:
    """
    Agent that bridges the hybrid message bus with Kraken execution.
    """
    
    def __init__(self, bus, config: KrakenConfig):
        self.bus = bus
        self.executor = KrakenExecutor(config)
        self.bus.subscribe(Channel.ORDERS, self._on_execution_request)
    
    async def start(self):
        await self.executor.initialize()
        print("[KrakenAgent] Started")
    
    async def stop(self):
        await self.executor.close()
    
    async def _on_execution_request(self, payload: dict):
        if payload.get("type") != "execute_order":
            return
        if payload.get("target") != "kraken":
            return
        
        data = payload["data"]
        order = OrderRequest(**data)
        
        result = await self.executor.place_order(order)
        
        self.bus.publish(Channel.ORDERS, {
            "type": "execution_result",
            "source": "kraken_agent",
            "data": {
                "success": result.success,
                "order_id": result.order_id,
                "symbol": result.symbol,
                "side": result.side,
                "volume": str(result.volume),
                "filled": str(result.filled_volume),
                "avg_price": str(result.avg_fill_price) if result.avg_fill_price else None,
                "status": result.status,
                "error": result.error
            }
        }, "kraken_agent")


# Add missing imports
import os
from hybrid.messaging import Channel

# =============================================================================
# USAGE EXAMPLE
# =============================================================================

async def example_usage():
    """Example: Using Kraken executor"""
    
    # Config from environment
    config = KrakenConfig(
        api_key=os.getenv("KRAKEN_API_KEY"),
        api_secret=os.getenv("KRAKEN_API_SECRET"),
        environment=KrakenEnvironment.SPOT
    )
    
    executor = KrakenExecutor(config)
    
    try:
        await executor.initialize()
        
        # Get ticker
        ticker = await executor.get_ticker("BTC/USD")
        print(f"BTC/USD: {ticker.last} (bid: {ticker.bid}, ask: {ticker.ask})")
        
        # Get balances
        balances = await executor.get_balances()
        for asset, bal in balances.items():
            if bal.total > 0:
                print(f"{asset}: {bal.total} (free: {bal.free})")
        
        # Place test order (validate only)
        result = await executor.place_order(OrderRequest(
            symbol="BTC/USD",
            side="buy",
            order_type="limit",
            volume=Decimal("0.001"),
            price=Decimal("50000"),
            validate_only=True  # Don't actually place
        ))
        print(f"Validate order: {result.success}")
        
    finally:
        await executor.close()


if __name__ == "__main__":
    asyncio.run(example_usage())
