"""
Orchestrator - Coordinates analysis schedule and turns approved decisions into orders
"""
import uuid

from hybrid.agent_base import BaseAgent
from hybrid.messaging import MessageBus, Channel

MIN_POSITION_USD = 10.0  # a smaller balance is dust, not a position


def kraken_balance_codes(ticker: str) -> set[str]:
    """Kraken spot balance codes for a pair's base asset: BTC/USDT -> {XBT, XXBT}.

    Older assets are X-prefixed (XXBT, XETH). Suffixed codes such as XBT.F or ETH.S
    are earn/staking balances, not spot, so they never match.
    """
    base = ticker.upper().replace("-", "/").split("/")[0]
    base = "XBT" if base == "BTC" else base
    return {base, f"X{base}"}


def holdings_key(dry_run: bool) -> str:
    """Redis key for the bot's positions. Simulated and live positions never share a key:
    a simulated position loaded in live mode could otherwise sell the user's real coins."""
    return f"orchestrator:holdings:{'dry_run' if dry_run else 'live'}"


class RedisHoldingsStore:
    """The bot's own positions (ticker -> volume) in a Redis hash, so it can close them after a restart."""

    def __init__(self, client, key: str):
        self.client = client  # redis.asyncio client with decode_responses=True
        self.key = key

    async def load(self) -> dict[str, float]:
        return {ticker: float(volume) for ticker, volume in (await self.client.hgetall(self.key)).items()}

    async def save(self, ticker: str, volume: float) -> None:
        await self.client.hset(self.key, ticker, str(volume))

    async def remove(self, ticker: str) -> None:
        await self.client.hdel(self.key, ticker)


class Orchestrator(BaseAgent):
    """Spot-only order policy for risk-approved quant decisions.

    BUY opens a long only when flat; SELL only closes a long we hold (spot can't
    short); nothing is sent for a ticker while its previous order is in flight.
    Holdings are the bot's own fills. With a ``store`` they are loaded on start and
    saved on every fill, so a restarted bot can still close its positions; a store
    outage is logged and trading continues from memory.

    Live mode (``exchange`` set): before each order the Kraken spot balance is
    checked. BUY is skipped if the account already holds the coin (e.g. from before
    a restart, or the user's own); SELL is capped at the actual balance and never
    touches coins the bot didn't buy; if the check fails, nothing is sent.
    Dry-run passes no exchange: simulated positions don't exist on Kraken.
    """

    def __init__(self, bus: MessageBus, config, exchange=None, store=None):
        super().__init__("orchestrator", bus)
        self.config = config
        self.exchange = exchange  # live Kraken spot executor, or None in dry-run
        self.store = store        # RedisHoldingsStore, or None to keep holdings in memory only
        self.holdings: dict[str, float] = {}  # ticker -> volume the bot bought and holds
        self.pending: dict[str, dict] = {}    # client_order_id -> {ticker, side, volume}
        self._checking: set[str] = set()      # tickers awaiting a balance check
        self.bus.subscribe(Channel.SIGNALS, self._on_signal)
        self.bus.subscribe(Channel.ORDERS, self._on_order)

    async def handle_message(self, payload: dict):
        pass

    async def start(self):
        if self.store:
            try:
                self.holdings = await self.store.load()
                print(f"[Orchestrator] Restored bot positions: {self.holdings or 'none'}")
            except Exception as e:
                print(f"[Orchestrator] ⚠️ Could not load saved positions ({e}); starting with none")
        print("[Orchestrator] Started")

    async def _on_signal(self, payload: dict):
        msg_type = payload.get("type")

        if msg_type == "quant_decision":
            print(f"[Orchestrator] Quant decision: {payload['data']['action']} {payload['data']['ticker']}")

        elif msg_type == "risk_assessment":
            data = payload['data']
            status = "✅ APPROVED" if data['approved'] else f"🚫 BLOCKED: {data['rejection_reason']}"
            print(f"[Orchestrator] Risk: {status} for {data['ticker']}")
            if data['approved']:
                await self._route_approved(data)

        elif msg_type == "llm_analysis_result":
            if payload['data']['success']:
                print(f"[Orchestrator] LLM analysis complete for {payload['data']['ticker']}")

        elif msg_type == "btc_funding_position":
            action = payload['data'].get('action', 'unknown')
            print(f"[Orchestrator] BTC Funding: {action} - {payload['data']}")

    async def _on_order(self, payload: dict):
        if payload.get("type") != "execution_result":
            return
        data = payload["data"]
        order = self.pending.pop(data.get("client_order_id"), None)
        status = "✅" if data["success"] else "❌"
        print(f"[Orchestrator] Execution: {status} {data['symbol']} - {data['message']}")
        if not order or not data["success"]:
            return
        if order["side"] == "buy":
            self.holdings[order["ticker"]] = order["volume"]
        else:
            self.holdings.pop(order["ticker"], None)
        await self._persist(order["ticker"])

    async def _route_approved(self, data: dict):
        ticker, action = data["ticker"], data.get("action")
        if ticker in self._checking or any(o["ticker"] == ticker for o in self.pending.values()):
            print(f"[Orchestrator] Skip {action} {ticker}: previous order still in flight")
            return
        held = self.holdings.get(ticker, 0.0)
        if action == "BUY" and held:
            print(f"[Orchestrator] Skip BUY {ticker}: already long")
            return
        if action == "SELL" and not held:
            print(f"[Orchestrator] Skip SELL {ticker}: no long position to close (spot, no shorting)")
            return
        if action not in ("BUY", "SELL"):
            return

        if self.exchange:
            self._checking.add(ticker)
            try:
                on_exchange = await self._spot_balance(ticker)
            except Exception as e:
                print(f"[Orchestrator] Skip {action} {ticker}: balance check failed, not trading ({e})")
                return
            finally:
                self._checking.discard(ticker)
            if action == "BUY" and on_exchange * float(data.get("price") or 0) >= MIN_POSITION_USD:
                print(f"[Orchestrator] Skip BUY {ticker}: account already holds {on_exchange} "
                      "(from before a restart, or not the bot's)")
                return
            if action == "SELL":
                held = min(held, on_exchange)
                if held <= 0:
                    self.holdings.pop(ticker, None)
                    await self._persist(ticker)
                    print(f"[Orchestrator] Skip SELL {ticker}: no balance left on Kraken; position dropped")
                    return

        if action == "BUY":
            self._send_order(ticker, "buy", data["shares"], data)
        else:
            self._send_order(ticker, "sell", held, data)

    async def _persist(self, ticker: str) -> None:
        if not self.store:
            return
        try:
            if ticker in self.holdings:
                await self.store.save(ticker, self.holdings[ticker])
            else:
                await self.store.remove(ticker)
        except Exception as e:
            print(f"[Orchestrator] ⚠️ Could not save position for {ticker} ({e}); "
                  "it is tracked in memory only and won't survive a restart")

    async def _spot_balance(self, ticker: str) -> float:
        """Base-asset spot balance on Kraken (total, including amounts held by open orders)."""
        balances = await self.exchange.get_balances()
        return sum(float(balances[code].total) for code in kraken_balance_codes(ticker) if code in balances)

    def _send_order(self, ticker: str, side: str, volume: float, data: dict):
        client_order_id = uuid.uuid4().hex[:16]
        self.pending[client_order_id] = {"ticker": ticker, "side": side, "volume": volume}
        self.bus.publish(Channel.ORDERS, {
            "type": "execute_order",
            "source": self.name,
            "data": {
                "symbol": ticker,
                "side": side,
                "volume": volume,
                "order_type": "market",
                "asset_type": "crypto" if "/" in ticker or "-" in ticker else "stock",
                "price": data.get("price"),
                "stop_loss": data.get("stop_loss"),
                "client_order_id": client_order_id,
            }
        }, self.name)
        print(f"[Orchestrator] Order sent: {side} {volume} {ticker} ({client_order_id})")

    async def analyze(self, ticker: str):
        self.bus.publish(Channel.SIGNALS, {
            "type": "analyze_request",
            "target": "quant_agent",
            "data": {"ticker": ticker}
        }, self.name)
