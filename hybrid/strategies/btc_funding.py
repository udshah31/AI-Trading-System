"""
BTC Funding Rate / Basis Trade Strategy
========================================
Delta-neutral strategy: Long Spot BTC + Short BTC Perpetual
Profits from positive funding rates (contango) on perpetual futures.
"""

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum

from hybrid.kraken_executor import KrakenExecutor, KrakenConfig, KrakenEnvironment, OrderRequest
from hybrid.messaging import MessageBus, Channel
from hybrid.config import HybridConfig


class PositionState(Enum):
    FLAT = "flat"
    ENTERING = "entering"
    OPEN = "open"
    CLOSING = "closing"
    ERROR = "error"


@dataclass
class FundingPosition:
    state: PositionState = PositionState.FLAT
    spot_volume: Decimal = Decimal("0")
    perp_volume: Decimal = Decimal("0")
    entry_spot_price: Decimal = Decimal("0")
    entry_perp_price: Decimal = Decimal("0")
    entry_funding_rate: Decimal = Decimal("0")
    entry_time: Optional[datetime] = None
    total_funding_collected: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")


class BTCFundingStrategy:
    """
    BTC Funding Rate Arbitrage Strategy
    
    Long Spot BTC + Short BTC Perpetual
    Collects funding payments when rate > threshold
    """
    
    def __init__(
        self,
        bus: MessageBus,
        config: HybridConfig,
        kraken_spot: KrakenExecutor,
        kraken_futures: KrakenExecutor,
        min_funding_bps: float = 1.0,
        max_basis_bps: float = 50.0,
        rebalance_interval_hours: int = 2,
        stop_loss_basis_bps: float = 200.0,
        max_position_usd: float = 10000.0,
        min_funding_apr: float = 5.0,
    ):
        self.bus = bus
        self.config = config
        self.kraken_spot = kraken_spot
        self.kraken_futures = kraken_futures
        
        self.min_funding_bps = min_funding_bps
        self.max_basis_bps = max_basis_bps
        self.rebalance_interval = rebalance_interval_hours * 3600
        self.stop_loss_basis_bps = stop_loss_basis_bps
        self.max_position_usd = Decimal(str(max_position_usd))
        self.min_funding_apr = min_funding_apr
        
        self.position = FundingPosition()
        self.running = False
        self._monitor_task: Optional[asyncio.Task] = None
        
        self.bus.subscribe(Channel.MARKET_DATA, self._on_market_data)
        self.bus.subscribe(Channel.SIGNALS, self._on_signals)
    
    async def start(self):
        self.running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        print("[BTCFunding] Strategy started")
    
    async def stop(self):
        self.running = False
        if self._monitor_task:
            self._monitor_task.cancel()
        if self.position.state == PositionState.OPEN:
            await self._close_position("strategy_stopped")
        print("[BTCFunding] Strategy stopped")
    
    async def _monitor_loop(self):
        emit_counter = 0
        while self.running:
            try:
                if self.position.state == PositionState.FLAT:
                    await self._check_entry()
                elif self.position.state == PositionState.OPEN:
                    await self._manage_position()
                
                # Emit strategy update every 30 seconds
                emit_counter += 1
                if emit_counter >= 30:
                    await self._emit_strategy_update()
                    emit_counter = 0
                    
                await asyncio.sleep(1)
            except Exception as e:
                print(f"[BTCFunding] Monitor error: {e}")
                await asyncio.sleep(1)
    
    async def _check_entry(self):
        funding_data = await self._get_funding_data()
        if not funding_data:
            return
        
        funding_rate_bps = funding_data["funding_rate"] * 10000
        funding_apr = funding_data["funding_rate"] * 365 * 3
        basis_bps = funding_data["basis_bps"]
        
        if (funding_rate_bps >= self.min_funding_bps and
            funding_apr >= self.min_funding_apr and
            abs(basis_bps) <= self.max_basis_bps):
            print(f"[BTCFunding] Entry: funding={funding_rate_bps:.1f}bps, APR={funding_apr:.1f}%, basis={basis_bps:.1f}bps")
            await self._enter_position(funding_data)
    
    async def _get_funding_data(self) -> Optional[Dict]:
        try:
            spot_ticker = await self.kraken_spot.get_ticker("XBT/USD")
            if not spot_ticker:
                return None
            spot_price = spot_ticker.last
            
            perp_ticker = await self.kraken_futures.get_ticker("PI_XBTUSD")
            if not perp_ticker:
                return None
            perp_price = perp_ticker.last
            
            basis = (perp_price - spot_price) / spot_price
            basis_bps = basis * 10000
            funding_rate = await self._get_funding_rate()
            
            return {
                "spot_price": spot_price,
                "perp_price": perp_price,
                "basis_bps": basis_bps,
                "funding_rate": funding_rate,
                "timestamp": datetime.utcnow()
            }
        except Exception as e:
            print(f"[BTCFunding] Error getting funding data: {e}")
            return None
    
    async def _get_funding_rate(self) -> Decimal:
        # ponytail: futures REST only, WS funding stream if we need sub-second
        try:
            data = await self.kraken_futures.rest.get_futures_tickers()
            for t in data.get("tickers", []):
                if t.get("symbol") == "PI_XBTUSD":
                    return Decimal(str(t.get("fundingRate", 0) or 0))
            return Decimal(str(data.get("fundingRate", 0) or 0))
        except Exception:
            return Decimal("0")
    
    async def _enter_position(self, funding_data: Dict):
        self.position.state = PositionState.ENTERING
        
        spot_price = funding_data["spot_price"]
        perp_price = funding_data["perp_price"]
        notional = min(self.max_position_usd, Decimal("10000"))
        spot_volume = (notional / spot_price).quantize(Decimal("0.00000001"))
        perp_volume = spot_volume
        
        print(f"[BTCFunding] Entering: Spot {spot_volume} BTC @ {spot_price}")
        
        try:
            spot_order = OrderRequest(symbol="XBT/USD", side="buy", order_type="market", volume=spot_volume)
            spot_result = await self.kraken_spot.place_order(spot_order)
            if not spot_result.success:
                raise Exception(f"Spot failed: {spot_result.error}")
            
            perp_order = OrderRequest(symbol="BTC/USD", side="sell", order_type="market", volume=perp_volume)
            perp_result = await self.kraken_futures.place_order(perp_order)
            if not perp_result.success:
                await self._emergency_close_spot(spot_volume)
                raise Exception(f"Perp failed: {perp_result.error}")
            
            self.position.state = PositionState.OPEN
            self.position.spot_volume = spot_volume
            self.position.perp_volume = perp_volume
            self.position.entry_spot_price = spot_price
            self.position.entry_perp_price = perp_price
            self.position.entry_funding_rate = funding_data["funding_rate"]
            self.position.entry_time = datetime.utcnow()
            
            print(f"[BTCFunding] ✅ Opened: {spot_volume} BTC")
            
        except Exception as e:
            self.position.state = PositionState.ERROR
            print(f"[BTCFunding] Entry failed: {e}")
            raise
    
    async def _manage_position(self):
        funding_data = await self._get_funding_data()
        if not funding_data:
            return
        
        spot_pnl = (funding_data["spot_price"] - self.position.entry_spot_price) * self.position.spot_volume
        perp_pnl = (self.position.entry_perp_price - funding_data["perp_price"]) * self.position.perp_volume
        total_pnl = spot_pnl + perp_pnl
        
        if self.position.entry_time:
            hours_held = (datetime.utcnow() - self.position.entry_time).total_seconds() / 3600
            periods = hours_held / 8
            funding_collected = self.position.spot_volume * self.position.entry_funding_rate * self.position.entry_spot_price * Decimal(str(periods))
            self.position.total_funding_collected = funding_collected
        
        self.position.unrealized_pnl = total_pnl
        
        # Exit conditions
        current_basis_bps = funding_data["basis_bps"]
        entry_basis_bps = ((self.position.entry_perp_price - self.position.entry_spot_price) / self.position.entry_spot_price * 10000)
        basis_change = current_basis_bps - entry_basis_bps
        
        should_exit = False
        exit_reason = ""
        
        if funding_data["funding_rate"] * 10000 < self.min_funding_bps * 0.5:
            should_exit, exit_reason = True, "funding_dropped"
        elif abs(current_basis_bps - ((self.position.entry_perp_price - self.position.entry_spot_price) / self.position.entry_spot_price * 10000)) > self.stop_loss_basis_bps:
            should_exit, exit_reason = True, "basis_widened"
        elif funding_data["funding_rate"] < 0:
            should_exit, exit_reason = True, "negative_funding"
        elif self.position.entry_time and (datetime.utcnow() - self.position.entry_time).days >= 7:
            should_exit, exit_reason = True, "max_hold"
        
        if should_exit:
            await self._close_position(exit_reason)
            return
        
        print(f"[BTCFunding] P&L={total_pnl:.2f}, Funding={self.position.total_funding_collected:.2f}, Basis={funding_data['basis_bps']:.1f}bps")
    
    async def _close_position(self, reason: str):
        self.position.state = PositionState.CLOSING
        try:
            spot_order = OrderRequest(symbol="XBT/USD", side="sell", order_type="market", volume=self.position.spot_volume)
            await self.kraken_spot.place_order(spot_order)
            
            perp_order = OrderRequest(symbol="BTC/USD", side="buy", order_type="market", volume=self.position.perp_volume)
            await self.kraken_futures.place_order(perp_order)
            
            final_pnl = self.position.unrealized_pnl + self.position.total_funding_collected
            print(f"[BTCFunding] Closed: {reason}, P&L={final_pnl:.2f}")
            
        except Exception as e:
            print(f"[BTCFunding] Close failed: {e}")
        finally:
            self.position = FundingPosition()
    
    async def _emergency_close_spot(self, volume: Decimal):
        try:
            order = OrderRequest(symbol="XBT/USD", side="sell", order_type="market", volume=volume)
            await self.kraken_spot.place_order(order)
        except Exception:
            pass
    
    async def _emit_strategy_update(self):
        """Emit strategy status update to dashboard"""
        try:
            funding_data = await self._get_funding_data()
            if not funding_data:
                return
            
            funding_rate_bps = funding_data["funding_rate"] * 10000
            funding_apr = funding_data["funding_rate"] * 365 * 3
            basis_bps = funding_data["basis_bps"]
            
            position_usd = Decimal("0")
            if self.position.state == PositionState.OPEN:
                position_usd = self.position.spot_volume * funding_data["spot_price"]
            
            await self.bus.publish("signals", {
                "type": "strategy_update",
                "data": {
                    "id": "btc_funding",
                    "status": "active" if self.position.state == PositionState.OPEN else "standby",
                    "funding_rate_bps": round(funding_rate_bps, 1),
                    "basis_bps": round(basis_bps, 1),
                    "apr_pct": round(funding_apr, 1),
                    "position_usd": float(position_usd),
                    "max_position_usd": float(self.max_position_usd),
                    "last_update": datetime.utcnow().isoformat() + "Z"
                }
            }, "btc_funding")
        except Exception as e:
            print(f"[BTCFunding] Emit error: {e}")

    async def _on_market_data(self, payload: Dict): pass
    async def _on_signals(self, payload: Dict): pass
    
    def get_status(self) -> Dict:
        return {
            "state": self.position.state.value,
            "spot_volume": str(self.position.spot_volume),
            "perp_volume": str(self.position.perp_volume),
            "unrealized_pnl": str(self.position.unrealized_pnl),
            "funding_collected": str(self.position.total_funding_collected),
            "entry_time": self.position.entry_time.isoformat() if self.position.entry_time else None
        }


async def create_btc_funding_strategy(bus, config, kraken_spot, kraken_futures, **kwargs):
    strategy = BTCFundingStrategy(bus, config, kraken_spot, kraken_futures)
    await strategy.start()
    return strategy
