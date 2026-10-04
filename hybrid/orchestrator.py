"""
Orchestrator - Coordinates analysis schedule and turns approved decisions into orders
"""
import asyncio
import json
import math
from typing import Optional
import secrets
import time
import uuid

from datetime import datetime, timezone

from hybrid.agent_base import BaseAgent
from hybrid.brokers.kraken import KrakenBroker, kraken_balance_codes  # noqa: F401  (re-exported)
from hybrid.messaging import MessageBus, Channel
from hybrid.portfolio import Portfolio

MIN_POSITION_USD = 10.0  # a smaller balance is dust, not a position
STOP_RETRY_S = 30.0      # after a failed stop-loss exit, wait this long before trying again
PORTFOLIO_PUBLISH_S = 5.0  # price-driven portfolio updates to the risk agent/dashboard, at most this often
PORTFOLIO_SAVE_S = 30.0    # price-driven saves of the books, at most this often (fills save at once)


def holdings_key(dry_run: bool, broker: str = "kraken") -> str:
    """Redis key for the bot's positions. Simulated, paper and live positions never share a
    key: a simulated position loaded in live mode could otherwise sell the user's real coins."""
    if broker == "alpaca":
        return "orchestrator:holdings:alpaca_paper"
    return f"orchestrator:holdings:{'dry_run' if dry_run else 'live'}"


def _valid_stop(stop) -> bool:
    try:
        return stop is not None and float(stop) > 0
    except (TypeError, ValueError):
        return False


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

    # Stop-loss price per held ticker
    async def load_stops(self) -> dict[str, float]:
        return {ticker: float(stop) for ticker, stop in (await self.client.hgetall(f"{self.key}:stops")).items()}

    async def save_stop(self, ticker: str, stop: float) -> None:
        await self.client.hset(f"{self.key}:stops", ticker, str(stop))

    async def remove_stop(self, ticker: str) -> None:
        await self.client.hdel(f"{self.key}:stops", ticker)

    # The bot's books (cash, positions, peak), so a restart keeps the high-water mark
    async def load_portfolio(self) -> Optional[dict]:
        state = (await self.client.hgetall(f"{self.key}:portfolio")).get("state")
        return json.loads(state) if state else None

    async def save_portfolio(self, state: dict) -> None:
        await self.client.hset(f"{self.key}:portfolio", "state", json.dumps(state))

    # Orders sent but not yet confirmed, written before sending (write-ahead)
    async def load_pending(self) -> dict[str, dict]:
        return {cid: json.loads(order) for cid, order in (await self.client.hgetall(f"{self.key}:pending")).items()}

    async def save_pending(self, client_order_id: str, order: dict) -> None:
        await self.client.hset(f"{self.key}:pending", client_order_id, json.dumps(order))

    async def remove_pending(self, client_order_id: str) -> None:
        await self.client.hdel(f"{self.key}:pending", client_order_id)


class Orchestrator(BaseAgent):
    """Spot-only order policy for risk-approved quant decisions.

    BUY opens a long only when flat; SELL only closes a long we hold (spot can't
    short); nothing is sent for a ticker while its previous order is in flight.
    Holdings are the bot's own fills. With a ``store`` they are loaded on start and
    saved on every fill, so a restarted bot can still close its positions; a store
    outage is logged and trading continues from memory.

    Each order is saved as pending before it is sent, tagged with a Kraken userref.
    On start, leftover pending orders (the bot died before their fill arrived) are
    settled against Kraken: filled -> position recorded or removed; never reached
    Kraken -> dropped; still open or lookup failed -> kept pending, which blocks new
    orders for that ticker. In live mode an order that can't be saved isn't sent.

    Live mode (``exchange`` set): before each order the Kraken spot balance is
    checked. BUY is skipped if the account already holds the coin (e.g. from before
    a restart, or the user's own); SELL is capped at the actual balance and never
    touches coins the bot didn't buy; if the check fails, nothing is sent.
    Dry-run passes no exchange: simulated positions don't exist on Kraken.

    Stop-losses: a BUY's stop price (from the risk assessment) is kept with the
    position and saved like it. Every live price tick is checked; when the bid (or
    last price) reaches the stop, the position is sold at market straight away. One
    exit at a time; a failed exit is retried after STOP_RETRY_S.

    Books: every fill and live price updates ``portfolio`` (cash, positions, equity,
    peak, drawdown). It is published as ``portfolio_update`` for the risk agent and the
    dashboard, and each fill as ``trade_filled`` with its realised P&L.
    """

    def __init__(self, bus: MessageBus, config, exchange=None, store=None, clock=time.monotonic, broker=None):
        super().__init__("orchestrator", bus)
        self.config = config
        # Broker whose real account state is checked (balances, order lookups): Alpaca paper,
        # live Kraken, or None for simulated dry-run. ``exchange`` (a KrakenExecutor) is the older form.
        self.broker = broker or (KrakenBroker(exchange) if exchange is not None else None)
        self.store = store        # RedisHoldingsStore, or None to keep holdings in memory only
        self.holdings: dict[str, float] = {}  # ticker -> volume the bot bought and holds
        self.pending: dict[str, dict] = {}    # client_order_id -> {ticker, side, volume, userref}
        self._checking: set[str] = set()      # tickers awaiting a balance check
        self._settlement_lock = asyncio.Lock()  # execution callbacks and reconciliation share one writer
        self.stops: dict[str, float] = {}     # ticker -> stop-loss price for the bot's position
        self._stop_retry_at: dict[str, float] = {}  # ticker -> earliest retry after a failed exit
        self._clock = clock
        self.portfolio = Portfolio(config.initial_capital)
        self._last_prices: dict[str, float] = {}
        self._last_publish = self._last_save = float("-inf")
        self.bus.subscribe(Channel.SIGNALS, self._on_signal)
        self.bus.subscribe(Channel.ORDERS, self._on_order)
        self.bus.subscribe(Channel.MARKET_DATA, self._on_market_data)

    async def handle_message(self, payload: dict):
        pass

    async def start(self):
        if self.store:
            try:
                self.holdings = await self.store.load()
                self.stops = {t: s for t, s in (await self.store.load_stops()).items() if t in self.holdings}
                print(f"[Orchestrator] Restored bot positions: {self.holdings or 'none'}; stops: {self.stops or 'none'}")
            except Exception as e:
                print(f"[Orchestrator] ⚠️ Could not load saved positions ({e}); starting with none")
            await self._load_portfolio()
            await self._reconcile_pending()
            for ticker in self.holdings:
                if ticker not in self.stops:
                    print(f"[Orchestrator] ⚠️ {ticker} has no stop-loss recorded; it only exits on a SELL decision")
        print("[Orchestrator] Started")

    async def _reconcile_pending(self, startup: bool = True):
        """Restore pending intents once, then poll only current unresolved orders."""
        async with self._settlement_lock:
            if startup and self.store:
                try:
                    self.pending.update(await self.store.load_pending())
                except Exception as e:
                    print(f"[Orchestrator] ⚠️ Could not load in-flight orders ({e})")
                    return
            await self._settle_pending(startup)

    async def _settle_pending(self, startup: bool):
        for cid, order in list(self.pending.items()):
            ticker, side = order["ticker"], order["side"]
            if not self.broker:
                if not startup:
                    continue  # simulated results may still be queued on the bus
                print(f"[Orchestrator] Dropping simulated in-flight {side} {ticker}: no exchange to check")
                await self._forget_pending(cid)
                continue
            if not startup and not order.get("awaiting_settlement") and time.time() - order.get("sent_at", 0) < 30:
                continue  # submission/polling is still owned by the executor
            try:
                found = await self.broker.find_orders(cid, order.get("userref"))
            except Exception as e:
                self.pending[cid] = order  # outcome unknown: keep blocking this ticker
                print(f"[Orchestrator] ⚠️ {side} {ticker} still unresolved, broker lookup failed ({e}); "
                      "not trading it until resolved")
                continue
            if not found:
                if not startup:
                    self._notify(f"{side.capitalize()} {ticker} not found on broker; outcome unknown, keeping pending",
                                 level="error")
                    continue  # a 404 during/after submission is not proof of rejection
                print(f"[Orchestrator] In-flight {side} {ticker} never reached the broker; dropped")
                await self._forget_pending(cid)
                continue
            terminal = {"closed", "filled", "partial", "canceled", "cancelled", "expired", "rejected"}
            if any(o.get("status") not in terminal for o in found):
                self.pending[cid] = order
                print(f"[Orchestrator] ⚠️ {side} {ticker} still open on Kraken; not trading it until it settles")
                continue
            volumes = [float(o.get("vol_exec") or 0) for o in found]
            if (any(not math.isfinite(v) or v < 0 for v in volumes)
                    or any(o.get("side") != side for o in found)):
                self._notify(f"Invalid broker fill for {ticker}; keeping unresolved", level="error")
                continue
            filled = sum(volumes)
            if filled > float(order["volume"]) + 1e-9:
                self._notify(f"Broker fill exceeds requested volume for {ticker}; keeping unresolved", level="error")
                continue
            numeric_prices: list[float] = []
            # Only old startup records may lack broker prices. Runtime settlement never invents one.
            for item, volume in zip(found, volumes):
                if volume <= 0:
                    continue
                raw_price = item.get("avg_price")
                if startup:
                    raw_price = raw_price or order.get("price")
                try:
                    price = float(raw_price)
                except (TypeError, ValueError):
                    numeric_prices = []
                    break
                if not math.isfinite(price) or price <= 0:
                    numeric_prices = []
                    break
                numeric_prices.append(price)
            fill_price = (sum(v * p for v, p in zip((v for v in volumes if v > 0), numeric_prices)) / filled
                          if filled > 0 and len(numeric_prices) == sum(v > 0 for v in volumes) else None)
            if filled > 0 and fill_price is None:
                print(f"[Orchestrator] ⚠️ {side} {ticker} filled but broker returned no fill price; keeping unresolved")
                continue
            if filled > 0:
                assert fill_price is not None
                realised = self.portfolio.apply_fill(ticker, side, filled, fill_price)
                self.bus.publish(Channel.SIGNALS, {"type": "trade_filled", "source": self.name, "data": {
                    "symbol": ticker, "side": side, "volume": filled, "price": float(fill_price),
                    "realized_pnl": realised, "client_order_id": cid,
                    "reason": order.get("reason", "reconciliation"), "exchange": self.broker.name}}, self.name)
            if filled > 0:
                if side == "buy":
                    self.holdings[ticker] = filled
                    if _valid_stop(order.get("stop")):
                        self.stops[ticker] = float(order["stop"])
                    await self._sync_to_broker(ticker)
                else:
                    remaining = self.holdings.get(ticker, 0.0) - filled
                    if remaining > 1e-12 and not self._is_dust(remaining, fill_price):
                        self.holdings[ticker] = remaining
                    else:
                        self.holdings.pop(ticker, None)
                        self.stops.pop(ticker, None)
                        self.portfolio.set_volume(ticker, 0.0)
                await self._persist(ticker)
                await self._portfolio_changed(force=True)
                print(f"[Orchestrator] Reconciled {side} {ticker}: filled {filled} @ {fill_price}")
            else:
                print(f"[Orchestrator] In-flight {side} {ticker} never filled on Kraken; dropped")
            await self._forget_pending(cid)

    async def reconcile_pending_orders(self):
        """Public periodic reconciliation hook for delayed broker fills."""
        await self._reconcile_pending(startup=False)

    async def _forget_pending(self, client_order_id: str):
        self.pending.pop(client_order_id, None)
        if self.store:
            try:
                await self.store.remove_pending(client_order_id)
            except Exception as e:
                print(f"[Orchestrator] ⚠️ Could not clear in-flight order {client_order_id} ({e})")

    async def _on_signal(self, payload: dict):
        msg_type = payload.get("type")

        if msg_type == "quant_decision":
            data = payload['data']
            age = data.get("llm_age_hours")
            source = f"LLM {age:.1f}h old" if age is not None else "technicals only"
            print(f"[Orchestrator] Quant decision: {data['action']} {data['ticker']} ({source})")

        elif msg_type == "risk_assessment":
            data = payload['data']
            status = "✅ APPROVED" if data['approved'] else f"🚫 BLOCKED: {data['rejection_reason']}"
            print(f"[Orchestrator] Risk: {status} for {data['ticker']}")
            if data['approved']:
                await self._route_approved(data)

        elif msg_type == "llm_analysis_result":
            data = payload['data']
            pair = data.get("pair") or data['ticker']
            if data['success']:
                print(f"[Orchestrator] LLM analysis complete for {pair} "
                      f"({data.get('duration_seconds') or 0:.0f}s)")
            else:
                print(f"[LLM] {pair} analysis failed: {data.get('error')}")

        elif msg_type == "btc_funding_position":
            action = payload['data'].get('action', 'unknown')
            print(f"[Orchestrator] BTC Funding: {action} - {payload['data']}")

    async def _on_order(self, payload: dict):
        if payload.get("type") != "execution_result":
            return
        async with self._settlement_lock:
            await self._process_execution_result(payload)

    async def _process_execution_result(self, payload: dict):
        data = payload["data"]
        cid = data.get("client_order_id")
        order = self.pending.get(cid)
        status = "✅" if data["success"] else "❌"
        print(f"[Orchestrator] Execution: {status} {data['symbol']} - {data['message']}")
        if not order:
            return
        ticker = order["ticker"]
        if data.get("status") == "open":
            order["awaiting_settlement"] = True
            # the broker hasn't filled it yet: keep it pending (blocks the coin) until it resolves
            self._notify(f"{order['side'].capitalize()} of {ticker} not filled yet; waiting before trading it again")
            return
        if data["success"]:
            filled = float(data.get("filled_volume") or order["volume"])
            await self._book_fill(order, data, filled)
            if order["side"] == "buy":
                self.holdings[ticker] = self.holdings.get(ticker, 0.0) + filled
                if _valid_stop(order.get("stop")):
                    self.stops[ticker] = float(order["stop"])
                await self._sync_to_broker(ticker)
            else:
                remaining = self.holdings.get(ticker, 0.0) - filled
                price = data.get("avg_price") or self._last_prices.get(ticker) or order.get("price")
                if remaining > 1e-12 and not self._is_dust(remaining, price):
                    self.holdings[ticker] = remaining  # partial sell: the rest stays, stop included
                else:
                    self.holdings.pop(ticker, None)
                    self.stops.pop(ticker, None)
                    self._stop_retry_at.pop(ticker, None)
                    self.portfolio.set_volume(ticker, 0.0)  # leftover dust is not a position
            await self._persist(ticker)  # position first, so a crash here is re-settled on start
        elif order.get("reason") == "stop-loss":
            self._stop_retry_at[ticker] = self._clock() + STOP_RETRY_S
            self._notify(f"Stop-loss sell of {ticker} failed ({data.get('message')}); retrying in "
                         f"{int(STOP_RETRY_S)}s", level="error")
        await self._forget_pending(cid)

    async def _on_market_data(self, payload: dict):
        """Value the books at live prices; sell a position the moment it reaches its stop-loss."""
        ticker = payload.get("symbol")
        if not isinstance(ticker, str):
            return
        mark = payload.get("bid") or payload.get("price")  # what a sell would fetch
        if mark:
            self._last_prices[ticker] = float(payload.get("price") or mark)
            self.portfolio.roll_day(datetime.now(timezone.utc).date())
            self.portfolio.mark(ticker, float(mark))
            await self._portfolio_changed(force=False)
        stop = self.stops.get(ticker)
        if stop is None or not self.holdings.get(ticker):
            return
        price = payload.get("bid") or payload.get("price")  # a market sell fills near the bid
        if price is None or float(price) > stop:
            return
        if ticker in self._checking or any(o["ticker"] == ticker for o in self.pending.values()):
            return  # exit (or another order) already in flight
        if self._clock() < self._stop_retry_at.get(ticker, 0.0):
            return
        await self._exit_on_stop(ticker, float(price), stop)

    async def _exit_on_stop(self, ticker: str, price: float, stop: float):
        held = self.holdings[ticker]
        if self.broker:
            self._checking.add(ticker)
            try:
                held = min(held, await self._spot_balance(ticker))
            except Exception as e:
                self._stop_retry_at[ticker] = self._clock() + STOP_RETRY_S
                self._notify(f"Stop-loss for {ticker} hit but the balance check failed ({e}); "
                             f"retrying in {int(STOP_RETRY_S)}s", level="error")
                return
            finally:
                self._checking.discard(ticker)
            if held <= 0:
                self.holdings.pop(ticker, None)
                self.stops.pop(ticker, None)
                await self._persist(ticker)
                self._notify(f"Stop-loss for {ticker} hit, but nothing is left on Kraken; position dropped")
                return
        self._notify(f"Stop-loss hit: {ticker} at {price:,.2f} (stop {stop:,.2f}). Selling {held}.", level="error")
        await self._send_order(ticker, "sell", held, {"price": price}, reason="stop-loss")

    async def _book_fill(self, order: dict, data: dict, volume: float):
        """Record a fill in the books at the best price known for it."""
        ticker, side = order["ticker"], order["side"]
        # exchange-reported fill price, else the latest live tick, else the order's (possibly stale) price
        price = data.get("avg_price") or self._last_prices.get(ticker) or order.get("price")
        if not price:
            print(f"[Orchestrator] ⚠️ No price for the {side} fill of {ticker}; books not updated")
            return
        realised = self.portfolio.apply_fill(ticker, side, volume, float(price))
        self.bus.publish(Channel.SIGNALS, {"type": "trade_filled", "source": self.name, "data": {
            "symbol": ticker, "side": side, "volume": volume, "price": float(price), "realized_pnl": realised,
            "client_order_id": data.get("client_order_id"), "reason": order.get("reason", "decision"),
            "exchange": data.get("exchange", ""),
        }}, self.name)
        await self._portfolio_changed(force=True)

    async def _portfolio_changed(self, force: bool):
        now = self._clock()
        if force or now - self._last_publish >= PORTFOLIO_PUBLISH_S:
            self._last_publish = now
            self.bus.publish(Channel.SIGNALS, {"type": "portfolio_update", "source": self.name,
                                               "data": self.portfolio.snapshot()}, self.name)
        if self.store and (force or now - self._last_save >= PORTFOLIO_SAVE_S):
            self._last_save = now
            try:
                await self.store.save_portfolio(self.portfolio.to_dict())
            except Exception as e:
                print(f"[Orchestrator] ⚠️ Could not save the books ({e})")

    async def _load_portfolio(self):
        try:
            state = await self.store.load_portfolio()
        except Exception as e:
            print(f"[Orchestrator] ⚠️ Could not load the books ({e}); starting fresh")
            state = None
        if state:
            self.portfolio = Portfolio.from_dict(state)
        for ticker, volume in self.holdings.items():  # positions held before the books existed
            if ticker not in self.portfolio.positions:
                self.portfolio.positions[ticker] = {"volume": volume, "avg_price": None, "last_price": None}
                print(f"[Orchestrator] {ticker} had no entry price on record; valuing it from the first price seen")
        snap = self.portfolio.snapshot()
        print(f"[Orchestrator] Books: equity {snap['equity']:,.2f}, peak {snap['peak']:,.2f}, "
              f"drawdown {snap['drawdown_pct']:.1%}")

    def _notify(self, message: str, level: str = "info"):
        """Print and show in the dashboard's activity log."""
        print(f"[Orchestrator] {message}")
        self.bus.publish(Channel.SIGNALS, {"type": "log", "data": {
            "source": "Orchestrator", "message": message, "level": level}}, self.name)

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

        if self.broker:
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
            await self._send_order(ticker, "buy", data["shares"], data)
        else:
            await self._send_order(ticker, "sell", held, data)

    async def _persist(self, ticker: str) -> None:
        if not self.store:
            return
        try:
            if ticker in self.holdings:
                await self.store.save(ticker, self.holdings[ticker])
            else:
                await self.store.remove(ticker)
            if ticker in self.stops:
                await self.store.save_stop(ticker, self.stops[ticker])
            else:
                await self.store.remove_stop(ticker)
        except Exception as e:
            print(f"[Orchestrator] ⚠️ Could not save position for {ticker} ({e}); "
                  "it is tracked in memory only and won't survive a restart")

    @staticmethod
    def _is_dust(volume: float, price) -> bool:
        """Too small to trade or to count as a position (e.g. a fee's worth left after a sell)."""
        return bool(price) and volume * float(price) < MIN_POSITION_USD

    async def _sync_to_broker(self, ticker: str) -> None:
        """After a buy, hold what the broker actually credited: Alpaca takes its crypto fee in
        the coin, so 0.0005 BTC bought is ~0.000499 held. Selling the booked 0.0005 would fail."""
        if not self.broker:
            return
        try:
            actual = await self._spot_balance(ticker)
        except Exception as e:
            print(f"[Orchestrator] ⚠️ Couldn't confirm the {ticker} position after the buy ({e}); "
                  "keeping the filled amount")
            return
        booked = self.holdings.get(ticker, 0.0)
        if 0 < actual < booked:  # never more than we booked: the rest of the account isn't the bot's
            self.holdings[ticker] = actual
            self.portfolio.set_volume(ticker, actual)
            await self._portfolio_changed(force=True)

    async def _spot_balance(self, ticker: str) -> float:
        """Base-asset spot balance on Kraken (total, including amounts held by open orders)."""
        assert self.broker is not None
        return await self.broker.position_volume(ticker)

    async def _send_order(self, ticker: str, side: str, volume: float, data: dict, reason: str = "decision"):
        client_order_id = uuid.uuid4().hex[:16]
        userref = secrets.randbelow(2**31 - 1) + 1  # Kraken userref is a positive int32
        order = {"ticker": ticker, "side": side, "volume": volume, "userref": userref, "sent_at": time.time(),
                 "reason": reason, "stop": data.get("stop_loss") if side == "buy" else None,
                 "price": self._last_prices.get(ticker) or data.get("price")}
        self.pending[client_order_id] = order  # in memory first: blocks this ticker while saving
        if self.store:
            try:
                await self.store.save_pending(client_order_id, order)
            except Exception as e:
                if self.broker:
                    self.pending.pop(client_order_id, None)
                    print(f"[Orchestrator] Skip {side} {ticker}: could not record the order before "
                          f"sending ({e}); a crash could lose track of it")
                    return
                print(f"[Orchestrator] ⚠️ Could not record simulated order ({e}); sending anyway")
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
                "userref": userref,
            }
        }, self.name)
        print(f"[Orchestrator] Order sent: {side} {volume} {ticker} ({client_order_id})")

    async def analyze(self, ticker: str):
        self.bus.publish(Channel.SIGNALS, {
            "type": "analyze_request",
            "target": "quant_agent",
            "data": {"ticker": ticker}
        }, self.name)
