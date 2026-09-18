"""
PostgreSQL Storage Layer
Async SQLAlchemy with Alembic migrations
"""
import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, List, Dict, Any
from uuid import uuid4

from sqlalchemy import (
    Column, String, Integer, BigInteger, Numeric, DateTime, 
    Text, Index, ForeignKey, Enum as SQLEnum, Boolean, JSON, text
)
from sqlalchemy.ext.asyncio import (
    create_async_engine, AsyncSession, async_sessionmaker
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB as PG_JSONB
from sqlalchemy.sql import func

# ponytail: String/JSON for PG+sqlite compat; restore PG_UUID/JSONB via dialect helper if PG perf needed
UUID = PG_UUID
JSONB = PG_JSONB

Base = declarative_base()


# =============================================================================
# MODELS
# =============================================================================

class Trade(Base):
    __tablename__ = "trades"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    side = Column(String(10), nullable=False)  # buy, sell
    volume = Column(Numeric(18, 8), nullable=False)
    price = Column(Numeric(18, 8), nullable=False)
    fee = Column(Numeric(18, 8), default=0)
    fee_currency = Column(String(10))
    pnl = Column(Numeric(18, 8), default=0)
    strategy = Column(String(50), index=True)
    exchange = Column(String(20))
    order_id = Column(String(100), index=True)
    client_order_id = Column(String(100))
    status = Column(String(20))  # open, closed, canceled, rejected
    trade_metadata = Column(JSON, default={})
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    __table_args__ = (
        Index('ix_trades_symbol_timestamp', 'symbol', 'timestamp'),
        Index('ix_trades_strategy_timestamp', 'strategy', 'timestamp'),
    )


class Position(Base):
    __tablename__ = "positions"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    symbol = Column(String(20), nullable=False, index=True)
    strategy = Column(String(50), index=True)
    exchange = Column(String(20))
    side = Column(String(10))  # long, short
    volume = Column(Numeric(18, 8), nullable=False)
    entry_price = Column(Numeric(18, 8), nullable=False)
    current_price = Column(Numeric(18, 8))
    unrealized_pnl = Column(Numeric(18, 8), default=0)
    realized_pnl = Column(Numeric(18, 8), default=0)
    stop_loss = Column(Numeric(18, 8))
    take_profit = Column(Numeric(18, 8))
    opened_at = Column(DateTime(timezone=True), nullable=False)
    closed_at = Column(DateTime(timezone=True))
    is_open = Column(Boolean, default=True, index=True)
    trade_metadata = Column(JSON, default={})


class EquityCurve(Base):
    __tablename__ = "equity_curve"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    account = Column(String(50), nullable=False, index=True)
    equity = Column(Numeric(18, 2), nullable=False)
    cash = Column(Numeric(18, 2))
    positions_value = Column(Numeric(18, 2))
    daily_pnl = Column(Numeric(18, 2))
    drawdown_pct = Column(Numeric(10, 6))
    trade_metadata = Column(JSON, default={})
    
    __table_args__ = (
        Index('ix_equity_account_timestamp', 'account', 'timestamp'),
    )


class Signal(Base):
    __tablename__ = "signals"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    agent = Column(String(50), nullable=False)
    signal_type = Column(String(50))  # quant, sentiment, technical, rl, sniper
    action = Column(String(20))  # buy, sell, hold
    strength = Column(Numeric(5, 4))  # 0-1
    confidence = Column(Numeric(5, 4))
    features = Column(JSON, default={})
    processed = Column(Boolean, default=False)
    executed = Column(Boolean, default=False)
    trade_id = Column(String(36), ForeignKey('trades.id'))
    
    __table_args__ = (
        Index('ix_signals_symbol_timestamp', 'symbol', 'timestamp'),
        Index('ix_signals_agent_timestamp', 'agent', 'timestamp'),
    )


class Order(Base):
    __tablename__ = "orders"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    side = Column(String(10), nullable=False)
    order_type = Column(String(20))  # market, limit, stop
    volume = Column(Numeric(18, 8), nullable=False)
    price = Column(Numeric(18, 8))
    filled_volume = Column(Numeric(18, 8), default=0)
    avg_fill_price = Column(Numeric(18, 8))
    status = Column(String(20), index=True)  # open, partial, filled, canceled, rejected
    exchange = Column(String(20))
    exchange_order_id = Column(String(100), index=True)
    client_order_id = Column(String(100))
    strategy = Column(String(50))
    trade_metadata = Column(JSON, default={})
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class StrategyPerformance(Base):
    __tablename__ = "strategy_performance"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    date = Column(DateTime(timezone=True), nullable=False, index=True)
    strategy = Column(String(50), nullable=False, index=True)
    symbol = Column(String(20), nullable=False)
    trades_count = Column(Integer, default=0)
    winning_trades = Column(Integer, default=0)
    losing_trades = Column(Integer, default=0)
    gross_profit = Column(Numeric(18, 2), default=0)
    gross_loss = Column(Numeric(18, 2), default=0)
    net_pnl = Column(Numeric(18, 2), default=0)
    max_drawdown = Column(Numeric(10, 6))
    sharpe_ratio = Column(Numeric(10, 4))
    win_rate = Column(Numeric(5, 4))
    profit_factor = Column(Numeric(10, 4))
    avg_trade = Column(Numeric(18, 2))
    best_trade = Column(Numeric(18, 2))
    worst_trade = Column(Numeric(18, 2))
    
    __table_args__ = (
        Index('ix_perf_strategy_date', 'strategy', 'date'),
        Index('ix_perf_symbol_date', 'symbol', 'date'),
    )


class AgentLog(Base):
    __tablename__ = "agent_logs"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    agent = Column(String(50), nullable=False, index=True)
    level = Column(String(10))  # debug, info, warning, error, critical
    message = Column(Text)
    context = Column(JSON, default={})
    trace_id = Column(String(50), index=True)
    span_id = Column(String(50))
    
    __table_args__ = (
        Index('ix_logs_agent_timestamp', 'agent', 'timestamp'),
        Index('ix_logs_trace', 'trace_id'),
    )


class MarketData(Base):
    __tablename__ = "market_data"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    exchange = Column(String(20), nullable=False)
    open = Column(Numeric(18, 8))
    high = Column(Numeric(18, 8))
    low = Column(Numeric(18, 8))
    close = Column(Numeric(18, 8), nullable=False)
    volume = Column(Numeric(24, 8))
    vwap = Column(Numeric(18, 8))
    bid = Column(Numeric(18, 8))
    ask = Column(Numeric(18, 8))
    timeframe = Column(String(10))  # 1m, 5m, 1h, 1d
    
    __table_args__ = (
        Index('ix_market_symbol_timestamp', 'symbol', 'timestamp'),
        Index('ix_market_exchange_symbol', 'exchange', 'symbol'),
    )


class SystemEvent(Base):
    __tablename__ = "system_events"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid4()))
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    event_type = Column(String(50), index=True)  # startup, shutdown, error, alert, config_change
    severity = Column(String(10))  # info, warning, critical
    source = Column(String(50))
    message = Column(Text)
    details = Column(JSON, default={})
    acknowledged = Column(Boolean, default=False)
    acknowledged_at = Column(DateTime(timezone=True))
    acknowledged_by = Column(String(50))


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
    async def session(self) -> AsyncSession:
        """Get a database session"""
        async with self.session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    
    async def execute_raw(self, query: str, params: dict = None):
        """Execute raw SQL"""
        async with self.session() as session:
            result = await session.execute(query, params or {})
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
                Trade.__table__.select()
                .where(Trade.symbol == symbol)
                .order_by(Trade.timestamp.desc())
                .limit(limit)
            )
            return result.scalars().all()
    
    async def get_by_strategy(self, strategy: str, days: int = 30) -> List[Trade]:
        async with self.db.session() as session:
            cutoff = datetime.utcnow() - timedelta(days=days)
            result = await session.execute(
                Trade.__table__.select()
                .where(Trade.strategy == strategy)
                .where(Trade.timestamp >= cutoff)
                .order_by(Trade.timestamp.desc())
            )
            return result.scalars().all()
    
    async def get_pnl_summary(self, strategy: str = None, days: int = 30) -> Dict:
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
            params = {"cutoff": cutoff}
            if strategy:
                query += " AND strategy = :strategy"
                params["strategy"] = strategy
            
            result = await session.execute(query, params)
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
    
    async def get_open_positions(self, strategy: str = None) -> List[Position]:
        async with self.db.session() as session:
            query = Position.__table__.select().where(Position.is_open == True)
            if strategy:
                query = query.where(Position.strategy == strategy)
            result = await session.execute(query)
            return result.scalars().all()
    
    async def close_position(self, position_id: str, close_price: Decimal, 
                            realized_pnl: Decimal) -> Position:
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
                EquityCurve.__table__.select()
                .where(EquityCurve.account == account)
                .order_by(EquityCurve.timestamp.desc())
                .limit(1)
            )
            return result.scalars().first()
    
    async def get_history(self, account: str, days: int = 30) -> List[EquityCurve]:
        async with self.db.session() as session:
            cutoff = datetime.utcnow() - timedelta(days=days)
            result = await session.execute(
                EquityCurve.__table__.select()
                .where(EquityCurve.account == account)
                .where(EquityCurve.timestamp >= cutoff)
                .order_by(EquityCurve.timestamp.asc())
            )
            return result.scalars().all()


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
                Signal.__table__.select()
                .where(Signal.processed == False)
                .order_by(Signal.timestamp.asc())
                .limit(limit)
            )
            return result.scalars().all()
    
    async def mark_processed(self, signal_id: str, trade_id: str = None):
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
                MarketData.__table__.select()
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
                MarketData.__table__.select()
                .where(MarketData.symbol == symbol)
                .where(MarketData.exchange == exchange)
                .where(MarketData.timeframe == timeframe)
                .where(MarketData.timestamp >= start)
                .where(MarketData.timestamp <= end)
                .order_by(MarketData.timestamp.asc())
            )
            return result.scalars().all()



class OrderRepository:
    def __init__(self, db: DatabaseManager):
        self.db = db
    
    async def save(self, order: Order) -> Order:
        async with self.db.session() as session:
            session.add(order)
            await session.flush()
            return order
    
    async def update_status(self, order_id: str, status: str, 
                          filled_volume: Decimal = None, avg_price: Decimal = None):
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
    
    async def record_market_data(self, **kwargs) -> MarketData:
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


