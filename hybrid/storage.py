"""
PostgreSQL Storage Layer
Async SQLAlchemy with Alembic migrations
"""
import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, AsyncIterator, Dict, List, Optional
from uuid import uuid4

from sqlalchemy import (
    Column, String, Integer, BigInteger, Numeric, DateTime, 
    Text, Index, ForeignKey, Enum as SQLEnum, Boolean, JSON, UniqueConstraint, text, Date
)
from sqlalchemy.ext.asyncio import (
    create_async_engine, AsyncSession, async_sessionmaker
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB as PG_JSONB
from sqlalchemy import select
from sqlalchemy.sql import func

# ponytail: String/JSON for PG+sqlite compat; restore PG_UUID/JSONB via dialect helper if PG perf needed
UUID = PG_UUID
JSONB = PG_JSONB

class Base(DeclarativeBase):
    pass


# =============================================================================
# MODELS
# =============================================================================

class Trade(Base):
    __tablename__ = "trades"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(10), nullable=False)  # buy, sell
    volume: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    fee: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), default=0)
    fee_currency: Mapped[Optional[str]] = mapped_column(String(10))
    pnl: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), default=0)
    strategy: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    exchange: Mapped[Optional[str]] = mapped_column(String(20))
    order_id: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    client_order_id: Mapped[Optional[str]] = mapped_column(String(100))
    status: Mapped[Optional[str]] = mapped_column(String(20))  # open, closed, canceled, rejected
    trade_metadata: Mapped[Optional[dict]] = mapped_column(JSON, default={})
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), server_default=func.now())
    
    __table_args__ = (
        Index('ix_trades_symbol_timestamp', 'symbol', 'timestamp'),
        Index('ix_trades_strategy_timestamp', 'strategy', 'timestamp'),
    )


class Position(Base):
    __tablename__ = "positions"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    strategy: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    exchange: Mapped[Optional[str]] = mapped_column(String(20))
    side: Mapped[Optional[str]] = mapped_column(String(10))  # long, short
    volume: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    entry_price: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    current_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8))
    unrealized_pnl: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), default=0)
    realized_pnl: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), default=0)
    stop_loss: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8))
    take_profit: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8))
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    closed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    is_open: Mapped[Optional[bool]] = mapped_column(Boolean, default=True, index=True)
    trade_metadata: Mapped[Optional[dict]] = mapped_column(JSON, default={})


class EquityCurve(Base):
    __tablename__ = "equity_curve"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    account: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    equity: Mapped[Decimal] = mapped_column(Numeric(18, 2), nullable=False)
    cash: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2))
    positions_value: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2))
    daily_pnl: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2))
    drawdown_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6))
    trade_metadata: Mapped[Optional[dict]] = mapped_column(JSON, default={})
    
    __table_args__ = (
        Index('ix_equity_account_timestamp', 'account', 'timestamp'),
    )


class Signal(Base):
    __tablename__ = "signals"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    agent: Mapped[str] = mapped_column(String(50), nullable=False)
    signal_type: Mapped[Optional[str]] = mapped_column(String(50))  # quant, sentiment, technical, rl, sniper
    action: Mapped[Optional[str]] = mapped_column(String(20))  # buy, sell, hold
    strength: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4))  # 0-1
    confidence: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4))
    features: Mapped[Optional[dict]] = mapped_column(JSON, default={})
    processed: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    executed: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    trade_id: Mapped[Optional[str]] = mapped_column(String(36), ForeignKey('trades.id'))
    
    __table_args__ = (
        Index('ix_signals_symbol_timestamp', 'symbol', 'timestamp'),
        Index('ix_signals_agent_timestamp', 'agent', 'timestamp'),
    )


class Order(Base):
    __tablename__ = "orders"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    side: Mapped[str] = mapped_column(String(10), nullable=False)
    order_type: Mapped[Optional[str]] = mapped_column(String(20))  # market, limit, stop
    volume: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    price: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8))
    filled_volume: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8), default=0)
    avg_fill_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8))
    status: Mapped[Optional[str]] = mapped_column(String(20), index=True)  # open, partial, filled, canceled, rejected
    exchange: Mapped[Optional[str]] = mapped_column(String(20))
    exchange_order_id: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    client_order_id: Mapped[Optional[str]] = mapped_column(String(100))
    strategy: Mapped[Optional[str]] = mapped_column(String(50))
    trade_metadata: Mapped[Optional[dict]] = mapped_column(JSON, default={})
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), onupdate=func.now())


class StrategyPerformance(Base):
    __tablename__ = "strategy_performance"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    strategy: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    trades_count: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    winning_trades: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    losing_trades: Mapped[Optional[int]] = mapped_column(Integer, default=0)
    gross_profit: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), default=0)
    gross_loss: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), default=0)
    net_pnl: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2), default=0)
    max_drawdown: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 6))
    sharpe_ratio: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4))
    win_rate: Mapped[Optional[Decimal]] = mapped_column(Numeric(5, 4))
    profit_factor: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4))
    avg_trade: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2))
    best_trade: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2))
    worst_trade: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 2))
    
    __table_args__ = (
        Index('ix_perf_strategy_date', 'strategy', 'date'),
        Index('ix_perf_symbol_date', 'symbol', 'date'),
    )


class AgentLog(Base):
    __tablename__ = "agent_logs"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    agent: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    level: Mapped[Optional[str]] = mapped_column(String(10))  # debug, info, warning, error, critical
    message: Mapped[Optional[str]] = mapped_column(Text)
    context: Mapped[Optional[dict]] = mapped_column(JSON, default={})
    trace_id: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    span_id: Mapped[Optional[str]] = mapped_column(String(50))
    
    __table_args__ = (
        Index('ix_logs_agent_timestamp', 'agent', 'timestamp'),
        Index('ix_logs_trace', 'trace_id'),
    )


class MarketData(Base):
    __tablename__ = "market_data"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    exchange: Mapped[str] = mapped_column(String(20), nullable=False)
    open: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8))
    high: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8))
    low: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8))
    close: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    volume: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    vwap: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8))
    bid: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8))
    ask: Mapped[Optional[Decimal]] = mapped_column(Numeric(18, 8))
    timeframe: Mapped[Optional[str]] = mapped_column(String(10))  # 1m, 5m, 1h, 1d
    
    __table_args__ = (
        Index('ix_market_symbol_timestamp', 'symbol', 'timestamp'),
        Index('ix_market_exchange_symbol', 'exchange', 'symbol'),
    )


class SystemEvent(Base):
    __tablename__ = "system_events"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    event_type: Mapped[Optional[str]] = mapped_column(String(50), index=True)  # startup, shutdown, error, alert, config_change
    severity: Mapped[Optional[str]] = mapped_column(String(10))  # info, warning, critical
    source: Mapped[Optional[str]] = mapped_column(String(50))
    message: Mapped[Optional[str]] = mapped_column(Text)
    details: Mapped[Optional[dict]] = mapped_column(JSON, default={})
    acknowledged: Mapped[Optional[bool]] = mapped_column(Boolean, default=False)
    acknowledged_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    acknowledged_by: Mapped[Optional[str]] = mapped_column(String(50))


class LLMAnalysis(Base):
    """One TradingAgents run: each signal's report text, extracted score, and how it was scored."""
    __tablename__ = "llm_analyses"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    ticker: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    trade_date: Mapped[str] = mapped_column(String(10), nullable=False)
    reports: Mapped[dict] = mapped_column(JSON, nullable=False)      # signal -> report text
    scores: Mapped[dict] = mapped_column(JSON, nullable=False)       # signal -> 0..1
    sources: Mapped[dict] = mapped_column(JSON, nullable=False)      # signal -> label|typesafe|unavailable
    confidences: Mapped[dict] = mapped_column(JSON, nullable=False)  # signal -> TypeSafe confidence


class SignalLabel(Base):
    """A person's direction for one report: the ground truth the extractor is measured against."""
    __tablename__ = "signal_labels"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    analysis_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("llm_analyses.id", ondelete="CASCADE"), nullable=False, index=True)
    signal: Mapped[str] = mapped_column(String(32), nullable=False)
    label: Mapped[str] = mapped_column(String(10), nullable=False)  # bearish | neutral | bullish
    labeled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    
    __table_args__ = (UniqueConstraint("analysis_id", "signal", name="uq_signal_labels_analysis_signal"),)


class DecisionOutcome(Base):
    """What happened after a quant decision: the price move over the next horizon."""
    __tablename__ = "decision_outcomes"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    signal_id: Mapped[str] = mapped_column(String(36), ForeignKey("signals.id", ondelete="CASCADE"),
                                           nullable=False, unique=True)
    horizon_hours: Mapped[int] = mapped_column(Integer, nullable=False)
    entry_price: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    exit_price: Mapped[Decimal] = mapped_column(Numeric(18, 8), nullable=False)
    forward_return: Mapped[Decimal] = mapped_column(Numeric(12, 8), nullable=False)
    labeled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class WeightProposal(Base):
    """Technical weights the learning loop suggests; applied only once a person approves."""
    __tablename__ = "weight_proposals"
    
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(12), nullable=False, index=True)  # pending|approved|rejected|superseded
    current: Mapped[dict] = mapped_column(JSON, nullable=False)   # relative weights when proposed
    proposed: Mapped[dict] = mapped_column(JSON, nullable=False)
    metrics: Mapped[dict] = mapped_column(JSON, nullable=False)
    decided_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class StockShadowDecision(Base):
    """Separate research ledger: never consumed by risk, execution or weight learning."""
    __tablename__ = "stock_shadow_decisions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    session_date: Mapped[date] = mapped_column(Date, nullable=False)
    session_close: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    analyzed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    action: Mapped[str] = mapped_column(String(10), nullable=False)
    score: Mapped[float] = mapped_column(Numeric(8, 6), nullable=False)
    confidence: Mapped[float] = mapped_column(Numeric(8, 6), nullable=False)
    close_price: Mapped[float] = mapped_column(Numeric(18, 8), nullable=False)
    features: Mapped[dict] = mapped_column(JSON, nullable=False)
    __table_args__ = (UniqueConstraint("symbol", "session_date", name="uq_stock_shadow_session"),)


# =============================================================================
# DATABASE MANAGER
# =============================================================================

class DatabaseManager:
    """Async database connection manager"""
    
    def __init__(self, database_url: str, echo: bool = False):
        # ponytail: guard only sqlite path, full dialect abstraction if we ever support MySQL
        is_sqlite = "sqlite" in database_url
        if is_sqlite:
            self.engine = create_async_engine(database_url, echo=echo)
        else:
            self.engine = create_async_engine(
                database_url,
                echo=echo,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600,
            )
        self.session_factory = async_sessionmaker(
            self.engine, class_=AsyncSession, expire_on_commit=False
        )
    
    async def initialize(self):
        """Create all tables"""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    
    async def close(self):
        await self.engine.dispose()
    
    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Get a database session"""
        async with self.session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    
    async def execute_raw(self, query: str, params: Optional[dict] = None):
        """Execute raw SQL"""
        async with self.session() as session:
            result = await session.execute(text(query), params or {})
            return result.fetchall()


# =============================================================================
# REPOSITORIES
# =============================================================================

class TradeRepository:
    """Trade data access"""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    async def save(self, trade: Trade) -> Trade:
        async with self.db.session() as session:
            session.add(trade)
            await session.flush()
            await session.refresh(trade)
            return trade
    
    async def save_batch(self, trades: List[Trade]) -> List[Trade]:
        async with self.db.session() as session:
            session.add_all(trades)
            await session.flush()
            for t in trades:
                await session.refresh(t)
            return trades
    
    async def get_by_symbol(self, symbol: str, limit: int = 100) -> List[Trade]:
        async with self.db.session() as session:
            result = await session.execute(
                select(Trade)
                .where(Trade.symbol == symbol)
                .order_by(Trade.timestamp.desc())
                .limit(limit)
            )
            return list(result.scalars().all())
    
    async def get_by_strategy(self, strategy: str, days: int = 30) -> List[Trade]:
        async with self.db.session() as session:
            cutoff = datetime.utcnow() - timedelta(days=days)
            result = await session.execute(
                select(Trade)
                .where(Trade.strategy == strategy)
                .where(Trade.timestamp >= cutoff)
                .order_by(Trade.timestamp.desc())
            )
            return list(result.scalars().all())
    
    async def get_pnl_summary(self, strategy: Optional[str] = None, days: int = 30) -> Dict:
        async with self.db.session() as session:
            cutoff = datetime.utcnow() - timedelta(days=days)
            query = """
                SELECT 
                    COUNT(*) as total_trades,
                    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                    SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
                    SUM(CASE WHEN pnl > 0 THEN pnl ELSE 0 END) as gross_profit,
                    SUM(CASE WHEN pnl < 0 THEN pnl ELSE 0 END) as gross_loss,
                    SUM(pnl) as net_pnl,
                    AVG(pnl) as avg_trade,
                    MAX(pnl) as best_trade,
                    MIN(pnl) as worst_trade
                FROM trades
                WHERE timestamp >= :cutoff
            """
            params: Dict[str, Any] = {"cutoff": cutoff}
            if strategy:
                query += " AND strategy = :strategy"
                params["strategy"] = strategy
            
            result = await session.execute(text(query), params)
            row = result.fetchone()
            return dict(row._mapping) if row else {}


class PositionRepository:
    """Position data access"""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    async def upsert(self, position: Position) -> Position:
        async with self.db.session() as session:
            existing = await session.get(Position, position.id)
            if existing:
                for attr in ['volume', 'current_price', 'unrealized_pnl', 
                           'realized_pnl', 'stop_loss', 'take_profit', 
                           'closed_at', 'is_open', 'position_metadata']:
                    setattr(existing, attr, getattr(position, attr))
                await session.flush()
                return existing
            else:
                session.add(position)
                await session.flush()
                await session.refresh(position)
                return position
    
    async def sync_open(self, strategy: str, positions: List[Dict], now: datetime) -> None:
        """Make the strategy's open rows match ``positions`` (update, open new, close gone)."""
        async with self.db.session() as session:
            result = await session.execute(
                select(Position).where(Position.strategy == strategy, Position.is_open == True))  # noqa: E712
            open_rows = {row.symbol: row for row in result.scalars()}
            for pos in positions:
                row = open_rows.pop(pos["symbol"], None)
                entry = Decimal(str(pos.get("avg_price") or pos.get("last_price") or 0))
                current = Decimal(str(pos["last_price"])) if pos.get("last_price") is not None else None
                if row is None:
                    row = Position(symbol=pos["symbol"], strategy=strategy, side="long", opened_at=now,
                                   is_open=True, volume=Decimal(0), entry_price=entry)
                    session.add(row)
                row.volume = Decimal(str(pos["volume"]))
                row.entry_price = entry
                row.current_price = current
                row.unrealized_pnl = Decimal(str(round(float(pos.get("unrealized_pnl", 0.0)), 8)))
            for row in open_rows.values():
                row.is_open, row.closed_at = False, now
    
    async def get_open_positions(self, strategy: Optional[str] = None) -> List[Position]:
        async with self.db.session() as session:
            query = select(Position).where(Position.is_open == True)
            if strategy:
                query = query.where(Position.strategy == strategy)
            result = await session.execute(query)
            return list(result.scalars().all())
    
    async def close_position(self, position_id: str, close_price: Decimal, 
                            realized_pnl: Decimal) -> Optional[Position]:
        async with self.db.session() as session:
            position = await session.get(Position, position_id)
            if position:
                position.is_open = False
                position.closed_at = datetime.utcnow()
                position.current_price = close_price
                position.realized_pnl = realized_pnl
                await session.flush()
            return position


class EquityRepository:
    """Equity curve data access"""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    async def save(self, equity: EquityCurve) -> EquityCurve:
        async with self.db.session() as session:
            session.add(equity)
            await session.flush()
            return equity
    
    async def get_latest(self, account: str) -> Optional[EquityCurve]:
        async with self.db.session() as session:
            result = await session.execute(
                select(EquityCurve)
                .where(EquityCurve.account == account)
                .order_by(EquityCurve.timestamp.desc())
                .limit(1)
            )
            return result.scalars().first()
    
    async def get_history(self, account: str, days: int = 30) -> List[EquityCurve]:
        async with self.db.session() as session:
            cutoff = datetime.utcnow() - timedelta(days=days)
            result = await session.execute(
                select(EquityCurve)
                .where(EquityCurve.account == account)
                .where(EquityCurve.timestamp >= cutoff)
                .order_by(EquityCurve.timestamp.asc())
            )
            return list(result.scalars().all())


class SignalRepository:
    """Signal data access"""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    async def save(self, signal: Signal) -> Signal:
        async with self.db.session() as session:
            session.add(signal)
            await session.flush()
            return signal
    
    async def get_unprocessed(self, limit: int = 100) -> List[Signal]:
        async with self.db.session() as session:
            result = await session.execute(
                select(Signal)
                .where(Signal.processed == False)
                .order_by(Signal.timestamp.asc())
                .limit(limit)
            )
            return list(result.scalars().all())
    
    async def mark_processed(self, signal_id: str, trade_id: Optional[str] = None):
        async with self.db.session() as session:
            signal = await session.get(Signal, signal_id)
            if signal:
                signal.processed = True
                if trade_id:
                    signal.executed = True
                    signal.trade_id = trade_id
                await session.flush()


class MarketDataRepository:
    """Market data access"""
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    async def save_batch(self, data: List[MarketData]) -> int:
        async with self.db.session() as session:
            session.add_all(data)
            await session.flush()
            return len(data)
    
    async def get_latest(self, symbol: str, exchange: str) -> Optional[MarketData]:
        async with self.db.session() as session:
            result = await session.execute(
                select(MarketData)
                .where(MarketData.symbol == symbol)
                .where(MarketData.exchange == exchange)
                .order_by(MarketData.timestamp.desc())
                .limit(1)
            )
            return result.scalars().first()
    
    async def get_ohlcv(self, symbol: str, exchange: str, 
                        start: datetime, end: datetime, 
                        timeframe: str = "1h") -> List[MarketData]:
        async with self.db.session() as session:
            result = await session.execute(
                select(MarketData)
                .where(MarketData.symbol == symbol)
                .where(MarketData.exchange == exchange)
                .where(MarketData.timeframe == timeframe)
                .where(MarketData.timestamp >= start)
                .where(MarketData.timestamp <= end)
                .order_by(MarketData.timestamp.asc())
            )
            return list(result.scalars().all())



class OrderRepository:
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    async def save(self, order: Order) -> Order:
        async with self.db.session() as session:
            session.add(order)
            await session.flush()
            return order
    
    async def update_status(self, order_id: str, status: str, 
                          filled_volume: Optional[Decimal] = None, avg_price: Optional[Decimal] = None):
        async with self.db.session() as session:
            order = await session.get(Order, order_id)
            if order:
                order.status = status
                if filled_volume is not None:
                    order.filled_volume = filled_volume
                if avg_price is not None:
                    order.avg_fill_price = avg_price
                order.updated_at = datetime.utcnow()
                await session.flush()


# =============================================================================
# HIGH-LEVEL STORAGE SERVICE
# =============================================================================

class LLMAnalysisRepository:
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    async def save_analysis(self, ticker: str, trade_date: str, final_state: Dict, signals: Any) -> str:
        """Store a TradingAgents run; ``signals`` is the LLMSignals extracted from it."""
        from hybrid.signal_extractor import SIGNAL_REPORTS
        analysis = LLMAnalysis(
            created_at=datetime.now(timezone.utc),
            ticker=ticker,
            trade_date=trade_date,
            reports={name: final_state.get(key) or "" for name, key in SIGNAL_REPORTS.items()},
            scores={name: float(getattr(signals, f"{name}_score")) for name in SIGNAL_REPORTS},
            sources=dict(signals.sources),
            confidences={k: float(v) for k, v in signals.confidences.items()},
        )
        async with self.db.session() as session:
            session.add(analysis)
            await session.flush()
            return analysis.id
    
    async def list_recent(self, limit: int = 50) -> List[tuple]:
        """[(analysis, labelled_count)], newest first."""
        labelled = (select(SignalLabel.analysis_id, func.count().label("n"))
                    .group_by(SignalLabel.analysis_id).subquery())
        async with self.db.session() as session:
            result = await session.execute(
                select(LLMAnalysis, func.coalesce(labelled.c.n, 0))
                .outerjoin(labelled, labelled.c.analysis_id == LLMAnalysis.id)
                .order_by(LLMAnalysis.created_at.desc())
                .limit(limit)
            )
            return [(row[0], int(row[1])) for row in result.all()]
    
    async def get(self, analysis_id: str) -> tuple:
        """(analysis or None, {signal: label})"""
        async with self.db.session() as session:
            analysis = await session.get(LLMAnalysis, analysis_id)
            labels = await session.execute(select(SignalLabel).where(SignalLabel.analysis_id == analysis_id))
            return analysis, {lab.signal: lab.label for lab in labels.scalars()}
    
    async def set_label(self, analysis_id: str, signal: str, label: str) -> None:
        async with self.db.session() as session:
            existing = (await session.execute(select(SignalLabel).where(
                SignalLabel.analysis_id == analysis_id, SignalLabel.signal == signal))).scalars().first()
            if existing:
                existing.label, existing.labeled_at = label, datetime.now(timezone.utc)
            else:
                session.add(SignalLabel(analysis_id=analysis_id, signal=signal, label=label,
                                        labeled_at=datetime.now(timezone.utc)))
    
    async def clear_label(self, analysis_id: str, signal: str) -> None:
        async with self.db.session() as session:
            existing = (await session.execute(select(SignalLabel).where(
                SignalLabel.analysis_id == analysis_id, SignalLabel.signal == signal))).scalars().first()
            if existing:
                await session.delete(existing)
    
    async def labelled_rows(self) -> List[Dict]:
        """Every labelled report with the extractor's reading, for signal_eval.accuracy()."""
        async with self.db.session() as session:
            result = await session.execute(
                select(SignalLabel, LLMAnalysis).join(LLMAnalysis, SignalLabel.analysis_id == LLMAnalysis.id))
            return [
                {"label": lab.label, "score": a.scores.get(lab.signal, 0.5),
                 "source": a.sources.get(lab.signal, "unavailable"),
                 "confidence": a.confidences.get(lab.signal), "text": a.reports.get(lab.signal, "")}
                for lab, a in result.all()
            ]


def _utc(ts: datetime) -> datetime:
    """SQLite returns naive datetimes; treat them as UTC like PostgreSQL's timestamptz."""
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


class LearningRepository:
    TECH_FEATURES = {"rsi": "rsi_score", "ema": "ema_crossover_score",
                     "bollinger": "bollinger_score", "volume": "volume_score"}
    
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    async def unlabeled_decisions(self, before: datetime) -> List[Signal]:
        """Quant decisions made at or before ``before`` that have no outcome yet."""
        async with self.db.session() as session:
            result = await session.execute(
                select(Signal)
                .outerjoin(DecisionOutcome, DecisionOutcome.signal_id == Signal.id)
                .where(Signal.agent == "quant_agent", Signal.timestamp <= before,
                       DecisionOutcome.id.is_(None))
                .order_by(Signal.timestamp.asc()))
            return list(result.scalars().all())
    
    async def save_outcome(self, signal_id: str, horizon_hours: int, entry_price: float,
                           exit_price: float) -> None:
        async with self.db.session() as session:
            session.add(DecisionOutcome(
                signal_id=signal_id, horizon_hours=horizon_hours,
                entry_price=Decimal(str(entry_price)), exit_price=Decimal(str(exit_price)),
                forward_return=Decimal(str(round(exit_price / entry_price - 1, 8))),
                labeled_at=datetime.now(timezone.utc)))
    
    async def labelled_samples(self) -> List[Dict]:
        """Decisions with outcomes: symbol, timestamp, technical scores, forward_return."""
        async with self.db.session() as session:
            result = await session.execute(
                select(Signal, DecisionOutcome).join(DecisionOutcome, DecisionOutcome.signal_id == Signal.id)
                .order_by(Signal.timestamp.asc()))
            samples = []
            for sig, outcome in result.all():
                features = sig.features or {}
                if not all(key in features for key in self.TECH_FEATURES.values()):
                    continue
                samples.append({
                    "symbol": sig.symbol, "timestamp": _utc(sig.timestamp),
                    "scores": {name: float(features[key]) for name, key in self.TECH_FEATURES.items()},
                    "forward_return": float(outcome.forward_return),
                })
            return samples
    
    async def save_proposal(self, current: Dict, proposed: Dict, metrics: Dict) -> str:
        """Store a pending proposal; an older pending one is superseded."""
        async with self.db.session() as session:
            pending = await session.execute(select(WeightProposal).where(WeightProposal.status == "pending"))
            for old in pending.scalars():
                old.status, old.decided_at = "superseded", datetime.now(timezone.utc)
            proposal = WeightProposal(created_at=datetime.now(timezone.utc), status="pending",
                                      current=dict(current), proposed=dict(proposed), metrics=dict(metrics))
            session.add(proposal)
            await session.flush()
            return proposal.id
    
    async def list_proposals(self, limit: int = 20) -> List[WeightProposal]:
        async with self.db.session() as session:
            result = await session.execute(
                select(WeightProposal).order_by(WeightProposal.created_at.desc()).limit(limit))
            return list(result.scalars().all())
    
    async def get_proposal(self, proposal_id: str) -> Optional[WeightProposal]:
        async with self.db.session() as session:
            return await session.get(WeightProposal, proposal_id)
    
    async def set_status(self, proposal_id: str, status: str) -> None:
        async with self.db.session() as session:
            proposal = await session.get(WeightProposal, proposal_id)
            if proposal:
                proposal.status, proposal.decided_at = status, datetime.now(timezone.utc)
    
    async def active_weights(self) -> Optional[Dict[str, float]]:
        """The most recently approved technical weights, or None if none approved yet."""
        async with self.db.session() as session:
            result = await session.execute(
                select(WeightProposal).where(WeightProposal.status == "approved")
                .order_by(WeightProposal.decided_at.desc()).limit(1))
            approved = result.scalars().first()
            return dict(approved.proposed) if approved else None


class StorageService:
    """Unified storage interface for all agents"""
    
    def __init__(self, database_url: str):
        self.db = DatabaseManager(database_url)
        self.trades = TradeRepository(self.db)
        self.positions = PositionRepository(self.db)
        self.equity = EquityRepository(self.db)
        self.signals = SignalRepository(self.db)
        self.orders = OrderRepository(self.db)  # Would need to create
        self.market_data = MarketDataRepository(self.db)
        self.llm = LLMAnalysisRepository(self.db)
        self.learning = LearningRepository(self.db)
    
    async def initialize(self):
        await self.db.initialize()
    
    async def close(self):
        await self.db.close()
    
    # Convenience methods
    async def record_trade(self, **kwargs) -> Trade:
        trade = Trade(**kwargs)
        return await self.trades.save(trade)
    
    async def record_signal(self, **kwargs) -> Signal:
        signal = Signal(**kwargs)
        return await self.signals.save(signal)
    
    async def update_position(self, **kwargs) -> Position:
        position = Position(**kwargs)
        return await self.positions.upsert(position)
    
    async def record_equity(self, **kwargs) -> EquityCurve:
        equity = EquityCurve(**kwargs)
        return await self.equity.save(equity)
    
    async def record_market_data(self, **kwargs) -> int:
        data = MarketData(**kwargs)
        return await self.market_data.save_batch([data])
    
    async def get_equity_history(self, account: str = "default", days: int = 30):
        """Get equity history for the dashboard"""
        from datetime import datetime, timezone, timedelta
        from sqlalchemy import select
        from hybrid.storage import EquityCurve
        
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        async with self.db.session() as session:
            result = await session.execute(
                select(EquityCurve)
                .where(EquityCurve.account == account)
                .where(EquityCurve.timestamp >= cutoff)
                .order_by(EquityCurve.timestamp.asc())
            )
            return result.scalars().all()


