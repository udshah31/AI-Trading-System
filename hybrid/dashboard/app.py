"""
Comprehensive Trading System Dashboard
Real-time monitoring UI for the entire trading system
"""
import asyncio
import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from hybrid.config import HybridConfig
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, field_validator


class UUIDStrModel(BaseModel):
    """Base model that coerces UUID ids to strings"""
    @field_validator("id", mode="before", check_fields=False)
    @classmethod
    def coerce_id(cls, v):
        return str(v) if v is not None else v


class TradeResponse(UUIDStrModel):
    id: str
    timestamp: datetime
    symbol: str
    side: str
    volume: float
    price: float
    fee: float
    pnl: float
    strategy: Optional[str] = None
    exchange: Optional[str] = None
    status: Optional[str] = None

class PositionResponse(UUIDStrModel):
    id: str
    symbol: str
    strategy: Optional[str] = None
    side: str
    volume: float
    entry_price: float
    current_price: Optional[float]
    unrealized_pnl: float
    realized_pnl: float
    is_open: bool
    opened_at: datetime
import redis.asyncio as redis

# Import storage
from hybrid.storage import StorageService, Trade, Position, EquityCurve, Signal

# Import metrics
from hybrid.metrics import registry, get_metrics


# =============================================================================
# CONFIGURATION
# =============================================================================

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://trader:secret@localhost:5432/trading")

storage: Optional[StorageService] = None
redis_client: Optional[redis.Redis] = None

# WebSocket connections
active_websockets: List[WebSocket] = []


# =============================================================================
# PYDANTIC MODELS (continued)
# =============================================================================

class EquityPoint(BaseModel):
    timestamp: datetime
    equity: float
    drawdown_pct: float

class SignalResponse(UUIDStrModel):
    id: str
    timestamp: datetime
    symbol: str
    agent: str
    signal_type: Optional[str] = None
    action: str
    strength: float
    confidence: float
    processed: bool
    executed: bool

class AgentStatus(BaseModel):
    name: str
    last_heartbeat: Optional[datetime]
    uptime_seconds: Optional[float]
    status: str  # healthy, stale, down
    message_count: int = 0
    error_count: int = 0

class SystemMetrics(BaseModel):
    total_equity: float
    daily_pnl: float
    open_positions: int
    total_trades_today: int
    win_rate: float
    max_drawdown: float
    sharpe_ratio: float
    active_strategies: int

class SystemOverview(BaseModel):
    metrics: SystemMetrics
    agents: List[AgentStatus]
    recent_trades: List[TradeResponse]
    open_positions: List[PositionResponse]
    recent_signals: List[SignalResponse]
    equity_curve: List[EquityPoint]


# =============================================================================
# LIFECYCLE
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global storage, redis_client
    
    # Initialize storage
    storage = StorageService(DATABASE_URL)
    await storage.initialize()
    
    # Initialize Redis
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    
    print("[Dashboard] Started on http://localhost:8000")
    
    yield
    
    # Cleanup
    await storage.close()
    await redis_client.close()
    print("[Dashboard] Stopped")


app = FastAPI(
    title="Trading System Dashboard",
    version="2.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# WEBSOCKET MANAGER
# =============================================================================

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
    
    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass
    
    async def send_personal(self, message: dict, websocket: WebSocket):
        try:
            await websocket.send_json(message)
        except:
            pass


manager = ConnectionManager()


async def metrics_broadcaster():
    """Broadcast metrics to WebSocket clients"""
    while True:
        try:
            if redis_client:
                data = await redis_client.hgetall("dashboard:latest")
                if data:
                    await manager.broadcast({
                        "type": "metrics_update",
                        "data": data,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })
        except Exception as e:
            print(f"Broadcast error: {e}")
# =============================================================================
# API ENDPOINTS
# =============================================================================

@app.get("/")
async def root():
    return HTMLResponse(content=open("hybrid/dashboard/index.html").read())


@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/metrics")
async def metrics():
    from fastapi.responses import Response
    return Response(content=get_metrics(), media_type="text/plain")


# --- System Overview ---
@app.get("/api/overview", response_model=SystemOverview)
async def get_overview():
    """Get complete system overview"""
    async with storage.db.session() as session:
        from sqlalchemy import select, func
        from hybrid.storage import Trade, Position, EquityCurve, Signal
        
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Today's trades
        trades_today = await session.execute(
            select(func.count(Trade.id)).where(Trade.timestamp >= today)
        )
        
        # Today's PnL
        pnl_today = await session.execute(
            select(func.sum(Trade.pnl)).where(Trade.timestamp >= today)
        )
        
        # Open positions
        open_pos = await session.execute(
            select(func.count(Position.id)).where(Position.is_open == True)
        )
        
        # Win rate (last 30 days)
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        win_stats = await session.execute(
            select(
                func.count(Trade.id).label("total"),
                func.sum(case((Trade.pnl > 0, 1), else_=0)).label("wins")
            ).where(Trade.timestamp >= cutoff)
        )
        win_row = win_stats.fetchone()
        win_rate = (win_row.wins / win_row.total) if win_row.total else 0
        
        # Latest equity
        latest_equity = await session.execute(
            select(EquityCurve.equity, EquityCurve.drawdown_pct)
            .order_by(EquityCurve.timestamp.desc())
            .limit(1)
        )
        equity_row = latest_equity.fetchone()
        
        # Get agents from Redis
        agents = []
        try:
            keys = await redis_client.keys("agent:heartbeat:*")
            for key in keys:
                agent_name = key.split(":")[-1]
                heartbeat = await redis_client.get(key)
                uptime = await redis_client.get(f"agent:uptime:{agent_name}")
                
                status = "down"
                last_hb = None
                if heartbeat:
                    last_hb = datetime.fromtimestamp(float(heartbeat), tz=timezone.utc)
                    if (datetime.now(timezone.utc) - last_hb).total_seconds() < 30:
                        status = "healthy"
                    else:
                        status = "stale"
                
                agents.append(AgentStatus(
                    name=agent_name,
                    last_heartbeat=last_hb,
                    uptime_seconds=float(uptime) if uptime else None,
                    status=status
                ))
        except Exception as e:
            print(f"Error getting agents: {e}")
        
        # Recent trades
        trades_result = await session.execute(
            select(Trade)
            .order_by(Trade.timestamp.desc())
            .limit(50)
        )
        recent_trades = trades_result.scalars().all()
        
        # Open positions
        pos_result = await session.execute(
            select(Position).where(Position.is_open == True).order_by(Position.opened_at.desc())
        )
        open_positions = pos_result.scalars().all()
        
        # Recent signals
        signals_result = await session.execute(
            select(Signal).order_by(Signal.timestamp.desc()).limit(20)
        )
        recent_signals = signals_result.scalars().all()
        
        # Equity curve (30 days)
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        eq_result = await session.execute(
            select(EquityCurve)
            .where(EquityCurve.timestamp >= cutoff)
            .order_by(EquityCurve.timestamp.asc())
        )
        equity_curve = eq_result.scalars().all()
        
        metrics = SystemMetrics(
            total_equity=float(equity_row.equity) if equity_row else 0,
            daily_pnl=float(pnl_today.scalar() or 0),
            open_positions=open_pos.scalar() or 0,
            total_trades_today=trades_today.scalar() or 0,
            win_rate=float(win_rate or 0),
            max_drawdown=float(equity_row.drawdown_pct or 0) if equity_row else 0,
            sharpe_ratio=0.0,
            active_strategies=4
        )
        
        return SystemOverview(
            metrics=metrics,
            agents=agents,
            recent_trades=recent_trades,
            open_positions=open_positions,
            recent_signals=recent_signals,
            equity_curve=equity_curve
        )


# --- Trades ---
@app.get("/api/trades", response_model=List[TradeResponse])
async def get_trades(
    symbol: Optional[str] = None,
    strategy: Optional[str] = None,
    limit: int = Query(100, le=1000),
    days: int = Query(30, le=365)
):
    async with storage.db.session() as session:
        from sqlalchemy import select, and_
        from hybrid.storage import Trade
        
        query = select(Trade).order_by(Trade.timestamp.desc()).limit(limit)
        
        if symbol:
            query = query.where(Trade.symbol == symbol)
        if strategy:
            query = query.where(Trade.strategy == strategy)
        if days:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            query = query.where(Trade.timestamp >= cutoff)
        
        result = await session.execute(query)
        return result.scalars().all()


@app.get("/api/trades/{trade_id}", response_model=TradeResponse)
async def get_trade(trade_id: str):
    async with storage.db.session() as session:
        from hybrid.storage import Trade
        trade = await session.get(Trade, trade_id)
        if not trade:
            raise HTTPException(404, "Trade not found")
        return trade


# --- Positions ---
@app.get("/api/positions", response_model=List[PositionResponse])
async def get_positions(open_only: bool = True):
    async with storage.db.session() as session:
        from sqlalchemy import select
        from hybrid.storage import Position
        
        query = select(Position)
        if open_only:
            query = query.where(Position.is_open == True)
        query = query.order_by(Position.opened_at.desc())
        
        result = await session.execute(query)
        return result.scalars().all()


@app.get("/api/positions/summary")
async def get_positions_summary():
    async with storage.db.session() as session:
        from sqlalchemy import select, func
        from hybrid.storage import Position
        
        # Open positions
        open_result = await session.execute(
            select(
                func.count(Position.id).label("count"),
                func.sum(Position.unrealized_pnl).label("unrealized_pnl"),
                func.sum(Position.volume * Position.current_price).label("total_value")
            ).where(Position.is_open == True)
        )
        open_stats = open_result.fetchone()
        
        # Today's realized PnL
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        closed_result = await session.execute(
            select(
                func.count(Position.id).label("count"),
                func.sum(Position.realized_pnl).label("realized_pnl")
            ).where(
                Position.is_open == False,
                Position.closed_at >= today
            )
        )
        closed_stats = closed_result.fetchone()
        
        return {
            "open_positions": open_stats.count or 0,
            "total_exposure": float(open_stats.total_value or 0),
            "unrealized_pnl": float(open_stats.unrealized_pnl or 0),
            "closed_today": closed_stats.count or 0,
            "realized_pnl_today": float(closed_stats.realized_pnl or 0)
        }


# --- Equity Curve ---
@app.get("/api/equity/{account}", response_model=List[EquityPoint])
async def get_equity(account: str, days: int = Query(30, le=365)):
    async with storage.db.session() as session:
        from sqlalchemy import select
        from hybrid.storage import EquityCurve
        
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        result = await session.execute(
            select(EquityCurve)
            .where(EquityCurve.account == account)
            .where(EquityCurve.timestamp >= cutoff)
            .order_by(EquityCurve.timestamp.asc())
        )
        return result.scalars().all()


# --- Signals ---
@app.get("/api/signals", response_model=List[SignalResponse])
async def get_signals(
    symbol: Optional[str] = None,
    agent: Optional[str] = None,
    limit: int = Query(100, le=500)
):
    async with storage.db.session() as session:
        from sqlalchemy import select, and_
        from hybrid.storage import Signal
        
        query = select(Signal).order_by(Signal.timestamp.desc()).limit(limit)
        
        if symbol:
            query = query.where(Signal.symbol == symbol)
        if agent:
            query = query.where(Signal.agent == agent)
        
        result = await session.execute(query)
        return result.scalars().all()


# --- Agents ---
@app.get("/api/agents", response_model=List[AgentStatus])
async def get_agents():
    """Get agent status from Redis heartbeats"""
    agents = []
    try:
        keys = await redis_client.keys("agent:heartbeat:*")
        for key in keys:
            agent_name = key.split(":")[-1]
            heartbeat = await redis_client.get(key)
            uptime = await redis_client.get(f"agent:uptime:{agent_name}")
            
            status = "down"
            last_hb = None
            if heartbeat:
                last_hb = datetime.fromtimestamp(float(heartbeat), tz=timezone.utc)
                if (datetime.now(timezone.utc) - last_hb).total_seconds() < 30:
                    status = "healthy"
                else:
                    status = "stale"
            
            agents.append(AgentStatus(
                name=agent_name,
                last_heartbeat=last_hb,
                uptime_seconds=float(uptime) if uptime else None,
                status=status
            ))
    except Exception as e:
        print(f"Error getting agents: {e}")
    
    return agents


# --- System Metrics ---
@app.get("/api/metrics/summary", response_model=SystemMetrics)
async def get_metrics_summary():
    """Get system-wide metrics"""
    async with storage.db.session() as session:
        from sqlalchemy import select, func
        from hybrid.storage import Trade, Position, EquityCurve
        
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        
        trades_today = await session.execute(
            select(func.count(Trade.id)).where(Trade.timestamp >= today)
        )
        
        pnl_today = await session.execute(
            select(func.sum(Trade.pnl)).where(Trade.timestamp >= today)
        )
        
        open_pos = await session.execute(
            select(func.count(Position.id)).where(Position.is_open == True)
        )
        
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        win_stats = await session.execute(
            select(
                func.count(Trade.id).label("total"),
                func.sum(case((Trade.pnl > 0, 1), else_=0)).label("wins")
            ).where(Trade.timestamp >= cutoff)
        )
        win_row = win_stats.fetchone()
        win_rate = (win_row.wins / win_row.total) if win_row.total else 0
        
        latest_equity = await session.execute(
            select(EquityCurve.equity, EquityCurve.drawdown_pct)
            .order_by(EquityCurve.timestamp.desc())
            .limit(1)
        )
        equity_row = latest_equity.fetchone()
        
        return SystemMetrics(
            total_equity=float(equity_row.equity) if equity_row else 0,
            daily_pnl=float(pnl_today.scalar() or 0),
            open_positions=open_pos.scalar() or 0,
            total_trades_today=trades_today.scalar() or 0,
            win_rate=float(win_rate or 0),
            max_drawdown=float(equity_row.drawdown_pct or 0) if equity_row else 0,
            sharpe_ratio=0.0,
            active_strategies=4
        )


# --- Redis Pub/Sub Data ---
@app.get("/api/realtime/{channel}")
async def get_realtime(channel: str):
    """Get latest data from Redis channel"""
    data = await redis_client.hgetall(f"dashboard:{channel}")
    return JSONResponse(content=data)


# =============================================================================
# WEBSOCKET
# =============================================================================

def _jsonable(value):
    """Recursively convert UUID/Decimal/datetime to JSON-safe types"""
    from uuid import UUID
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items() if not k.startswith("_")}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Send initial data
        async with storage.db.session() as session:
            from sqlalchemy import select
            from hybrid.storage import Position, EquityCurve
            
            # Open positions
            pos_result = await session.execute(
                select(Position).where(Position.is_open == True)
            )
            positions = pos_result.scalars().all()
            
            # Latest equity
            eq_result = await session.execute(
                select(EquityCurve).order_by(EquityCurve.timestamp.desc()).limit(1)
            )
            equity = eq_result.scalars().first()
            
            await websocket.send_json({
                "type": "initial",
                "data": {
                    "positions": [_jsonable(p.__dict__) for p in positions],
                    "equity": _jsonable(equity.__dict__) if equity else None
                }
            })
        
        # Keep alive
        while True:
            try:
                msg = await websocket.receive_text()
                # Handle client messages if needed
            except WebSocketDisconnect:
                break
            except Exception:
                break
    finally:
        manager.disconnect(websocket)


# =============================================================================
# BACKGROUND TASKS
# =============================================================================

async def update_redis_cache():
    """Periodically update Redis with latest data for real-time UI"""
    while True:
        try:
            if not storage:
                await asyncio.sleep(5)
                continue
            
            async with storage.db.session() as session:
                from sqlalchemy import select, func
                from hybrid.storage import Position, EquityCurve, Trade
                
                # Open positions
                pos_result = await session.execute(
                    select(Position).where(Position.is_open == True)
                )
                positions = [{
                    "id": str(p.id),
                    "symbol": p.symbol,
                    "strategy": p.strategy,
                    "side": p.side,
                    "volume": float(p.volume),
                    "entry_price": float(p.entry_price),
                    "current_price": float(p.current_price) if p.current_price else None,
                    "unrealized_pnl": float(p.unrealized_pnl),
                    "is_open": p.is_open
                } for p in pos_result.scalars().all()]
                
                # Latest equity
                eq_result = await session.execute(
                    select(EquityCurve).order_by(EquityCurve.timestamp.desc()).limit(1)
                )
                equity = eq_result.scalars().first()
                
                # Today's trades count
                today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                trades_today = await session.execute(
                    select(func.count(Trade.id)).where(Trade.timestamp >= today)
                )
                
                # Today's PnL
                pnl_today = await session.execute(
                    select(func.sum(Trade.pnl)).where(Trade.timestamp >= today)
                )
                
                data = {
                    "positions": json.dumps(positions),
                    "equity": json.dumps({
                        "total": float(equity.equity) if equity else 0,
                        "drawdown": float(equity.drawdown_pct) if equity else 0
                    }) if equity else "{}",
                    "trades_today": str(trades_today.scalar() or 0),
                    "pnl_today": str(pnl_today.scalar() or 0),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                
                await redis_client.hset("dashboard:latest", mapping=data)
                
        except Exception as e:
            print(f"Redis cache update error: {e}")
        
        await asyncio.sleep(2)


# Add missing import
from sqlalchemy import func, case

# Start background tasks
@app.on_event("startup")
async def startup_tasks():
    asyncio.create_task(metrics_broadcaster())
    asyncio.create_task(update_redis_cache())


# --- Strategies ---
@app.get("/api/strategies")
async def get_strategies():
    """Get configured strategies and their status"""
    # This would ideally come from a registry, for now return static config
    return [
        {
            "id": "quant",
            "name": "Quant Pipeline",
            "description": "RSI + EMA + Bollinger + Volume",
            "status": "active",
            "params": { "RSI": "14", "EMA": "12/26", "BBands": "20,2", "Volume": "20 SMA" }
        },
        {
            "id": "btc_funding",
            "name": "BTC Funding Arb",
            "description": "Delta-neutral: Spot + Perp Short",
            "status": "active",
            "params": { "Funding": "10 bps/8hr", "APR": "~45%", "Basis": "6.3 bps", "Max Pos": "$10k" }
        },
        {
            "id": "sniper",
            "name": "Sniper Bot",
            "description": "DEX New Token Hunter",
            "status": "standby",
            "params": { "Chains": "Solana, Base", "Max Pos": "$500", "TP/SL": "50%/30%" }
        },
        {
            "id": "rl",
            "name": "RL Agent (PPO)",
            "description": "Portfolio Allocation Policy",
            "status": "training",
            "params": { "Algo": "PPO", "Obs Dim": "12", "Action": "[-1, 1]" }
        }
    ]


# --- Equity History ---
@app.get("/api/equity/history")
async def get_equity_history(days: int = Query(30, le=365), account: str = "default"):
    async with storage.db.session() as session:
        from sqlalchemy import select
        from hybrid.storage import EquityCurve
        
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        result = await session.execute(
            select(EquityCurve)
            .where(EquityCurve.account == account)
            .where(EquityCurve.timestamp >= cutoff)
            .order_by(EquityCurve.timestamp.asc())
        )
        points = result.scalars().all()
        return {
            "labels": [p.timestamp.isoformat() for p in points],
            "data": [float(p.equity) for p in points]
        }


# --- Risk Metrics ---
@app.get("/api/risk")
async def get_risk_metrics():
    """Get live risk metrics"""
    if not storage:
        return {"error": "Storage not available"}
    try:
        async with storage.db.session() as session:
            from sqlalchemy import select, func
            from hybrid.storage import Trade, Position, EquityCurve
            
            today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            
            # Daily P&L
            pnl_today = await session.execute(
                select(func.sum(Trade.pnl)).where(Trade.timestamp >= today)
            )
            daily_pnl = float(pnl_today.scalar() or 0)
            
            # Total equity
            equity_result = await session.execute(
                select(EquityCurve).order_by(EquityCurve.timestamp.desc()).limit(1)
            )
            equity_row = equity_result.scalars().first()
            total_equity = float(equity_row.equity) if equity_row else 0
            
            # Drawdown
            max_drawdown = float(equity_row.drawdown_pct or 0) if equity_row else 0
            
            # Open positions
            pos_result = await session.execute(
                select(func.count(Position.id)).where(Position.is_open == True)
            )
            position_count = pos_result.scalar() or 0
            
            # Total exposure
            exposure_result = await session.execute(
                select(func.sum(Position.volume * Position.current_price)).where(Position.is_open == True)
            )
            total_exposure = float(exposure_result.scalar() or 0)
            
            # Risk metrics
            config = HybridConfig()
            max_drawdown_limit = config.max_drawdown_pct * 100
            max_position_pct = config.max_position_pct * 100
            risk_per_trade = config.max_risk_per_trade_pct * total_equity
            max_daily_loss = config.max_drawdown_pct * total_equity
            risk_budget_remaining = max(0, max_daily_loss + daily_pnl)
            margin_used_pct = (total_exposure / total_equity * 100) if total_equity else 0
            circuit_breaker = max_drawdown >= max_drawdown_limit
            
            return {
                "drawdown_pct": round(max_drawdown, 2),
                "max_drawdown_pct": max_drawdown_limit,
                "daily_pnl": round(daily_pnl, 2),
                "max_daily_loss": round(max_daily_loss, 2),
                "margin_used_pct": round(margin_used_pct, 1),
                "max_position_pct": max_position_pct,
                "current_positions": position_count,
                "total_exposure_usd": round(total_exposure, 2),
                "risk_budget_remaining": round(risk_budget_remaining, 2),
                "circuit_breaker": circuit_breaker,
                "total_equity": round(total_equity, 2),
                "daily_pnl": round(daily_pnl, 2)
            }
    except Exception as e:
        print(f"[Risk API] Error: {e}")
        return {"error": str(e)}


# --- Live Strategy Status ---
@app.get("/api/strategies/live")
async def get_live_strategies():
    """Get live strategy status from agents (not static config)"""
    # This would ideally come from agent state via Redis
    # For now, return static with note
    return [
        {
            "id": "quant",
            "name": "Quant Pipeline",
            "description": "RSI + EMA + Bollinger + Volume",
            "status": "active",
            "params": { "RSI": "14", "EMA": "12/26", "BBands": "20,2", "Volume": "20 SMA" },
            "source": "static_config"
        },
        {
            "id": "btc_funding",
            "name": "BTC Funding Arb",
            "description": "Delta-neutral: Spot + Perp Short",
            "status": "active",
            "params": { "Funding": "10 bps/8hr", "APR": "~45%", "Basis": "6.3 bps", "Max Pos": "$10k" },
            "source": "live_agent"
        },
        {
            "id": "sniper",
            "name": "Sniper Bot",
            "description": "DEX New Token Hunter",
            "status": "standby",
            "params": { "Chains": "Solana, Base", "Max Pos": "$500", "TP/SL": "50%/30%" },
            "source": "live_agent"
        },
        {
            "id": "rl",
            "name": "RL Agent (PPO)",
            "description": "Portfolio Allocation Policy",
            "status": "training",
            "params": { "Algo": "PPO", "Obs Dim": "12", "Action": "[-1, 1]" },
            "source": "static_config"
        }
    ]


# --- Sniper Alerts ---
@app.get("/api/sniper/alerts")
async def get_sniper_alerts(limit: int = Query(20, le=100)):
    """Get recent sniper alerts"""
    # In production, this would come from a dedicated alerts table or Redis
    # For now, return empty
    return []


# --- Backtest Results ---
@app.get("/api/backtest/results")
async def get_backtest_results(strategy_id: str, days: int = Query(30, le=365)):
    """Get backtest results for a strategy"""
    if not storage:
        return {"error": "Storage not available"}
    try:
        async with storage.db.session() as session:
            from sqlalchemy import select, and_
            from hybrid.storage import Trade, EquityCurve
            
            # Filter trades by strategy
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            query = select(Trade).where(Trade.timestamp >= cutoff)
            if strategy_id:
                query = query.where(Trade.strategy == strategy_id)
            query = query.order_by(Trade.timestamp.asc())
            
            result = await session.execute(query)
            trades = result.scalars().all()
            
            # Build equity curve from trades
            equity_points = []
            running_equity = 100000.0  # Starting capital
            for trade in trades:
                running_equity += float(trade.pnl)
                equity_points.append({
                    "timestamp": trade.timestamp.isoformat(),
                    "equity": running_equity,
                    "trade_pnl": float(trade.pnl)
                })
            
            return {
                "strategy_id": strategy_id,
                "days": days,
                "total_trades": len(trades),
                "total_pnl": sum(float(t.pnl) for t in trades),
                "equity_curve": equity_points,
                "trades": [
                    {
                        "timestamp": t.timestamp.isoformat(),
                        "symbol": t.symbol,
                        "side": t.side,
                        "volume": float(t.volume),
                        "price": float(t.price),
                        "pnl": float(t.pnl)
                    } for t in trades
                ]
            }
    except Exception as e:
        print(f"[Backtest API] Error: {e}")
        return {"error": str(e)}


# --- ML Optimizer State ---
@app.get("/api/optimizer/state")
async def get_optimizer_state():
    """Get ML optimizer current state"""
    try:
        # Load learned weights
        import json
        from pathlib import Path
        weights_file = Path("hybrid/learned_weights.json")
        if weights_file.exists():
            with open(weights_file) as f:
                weights = json.load(f)
        else:
            weights = {}
        
        return {
            "weights": weights,
            "last_updated": "unknown",
            "status": "ready" if weights else "not_trained"
        }
    except Exception as e:
        return {"error": str(e)}


# --- RL Trainer State ---
@app.get("/api/rl/state")
async def get_rl_state():
    """Get RL trainer current state"""
    # In production, this would read from RL trainer state file
    return {
        "episode": 0,
        "total_reward": 0.0,
        "loss": 0.0,
        "entropy": 0.0,
        "allocation_weights": {},
        "status": "not_running"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
