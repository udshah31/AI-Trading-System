"""
Comprehensive Trading System Dashboard
Real-time monitoring UI for the entire trading system
"""
import asyncio
import base64
import binascii
import json
import os
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Literal, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response
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


def _db() -> StorageService:
    """The storage service; set by lifespan() before any request is served."""
    if storage is None:
        raise RuntimeError("Dashboard storage not initialized")
    return storage


def _redis() -> redis.Redis:
    if redis_client is None:
        raise RuntimeError("Dashboard Redis client not initialized")
    return redis_client

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
    
    if not os.getenv("DASHBOARD_PASSWORD"):
        raise RuntimeError("DASHBOARD_PASSWORD is not set; refusing to serve the dashboard without auth")
    
    # Initialize storage
    storage = StorageService(DATABASE_URL)
    await storage.initialize()
    
    # Initialize Redis
    redis_client = redis.from_url(REDIS_URL, decode_responses=True)
    
    # on_event("startup") handlers are ignored when lifespan= is set, so start tasks here
    tasks = [
        asyncio.create_task(metrics_broadcaster()),
        asyncio.create_task(update_redis_cache()),
        asyncio.create_task(signals_relay()),
    ]
    
    print("[Dashboard] Started on http://localhost:8000")
    
    yield
    
    # Cleanup
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await storage.close()
    await redis_client.close()
    print("[Dashboard] Stopped")


class BasicAuthMiddleware:
    """HTTP Basic auth for every HTTP and WebSocket route except PUBLIC_PATHS.

    Credentials come from DASHBOARD_USER (default "admin") / DASHBOARD_PASSWORD,
    read per request. With no password configured every protected request is
    refused, so a misconfigured deploy fails closed.
    """

    PUBLIC_PATHS = {"/health"}  # deploy health check

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] not in ("http", "websocket") or scope["path"] in self.PUBLIC_PATHS:
            return await self.app(scope, receive, send)

        password = os.getenv("DASHBOARD_PASSWORD", "")
        if password and self._authorized(scope, os.getenv("DASHBOARD_USER", "admin"), password):
            return await self.app(scope, receive, send)

        if scope["type"] == "websocket":
            await receive()  # websocket.connect
            await send({"type": "websocket.close", "code": 1008})  # policy violation
        elif not password:
            await PlainTextResponse("Dashboard auth not configured", status_code=503)(scope, receive, send)
        else:
            await PlainTextResponse(
                "Unauthorized", status_code=401,
                headers={"WWW-Authenticate": 'Basic realm="Trading Dashboard"'},
            )(scope, receive, send)

    @staticmethod
    def _authorized(scope, username: str, password: str) -> bool:
        header = dict(scope.get("headers") or []).get(b"authorization", b"")
        scheme, _, encoded = header.partition(b" ")
        if scheme.lower() != b"basic":
            return False
        try:
            user, _, given = base64.b64decode(encoded, validate=True).partition(b":")
        except (binascii.Error, ValueError):
            return False
        # evaluate both comparisons so timing doesn't reveal which one failed
        user_ok = secrets.compare_digest(user, username.encode())
        password_ok = secrets.compare_digest(given, password.encode())
        return user_ok and password_ok


app = FastAPI(
    title="Trading System Dashboard",
    version="2.0.0",
    lifespan=lifespan
)

app.add_middleware(BasicAuthMiddleware)

# The UI is same-origin, so CORS is off unless origins are listed explicitly.
# Added after auth so it wraps it and can answer credential-less preflights.
_cors_origins = [o.strip() for o in os.getenv("DASHBOARD_CORS_ORIGINS", "").split(",") if o.strip()]
if _cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_cors_origins,
        allow_credentials=True,
        allow_methods=["GET"],
        allow_headers=["Authorization"],
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
        await asyncio.sleep(2)


# Agent message types the dashboard UI renders (see index.html ws.onmessage)
RELAYED_SIGNAL_TYPES = {"equity_history", "strategy_update", "risk_update", "sniper_alert", "quant_decision", "log"}


def relay_message(raw: str) -> Optional[dict]:
    """Turn a MessageBus envelope from Redis into a WebSocket message, or None to drop it."""
    try:
        payload = json.loads(raw).get("payload", {})
    except (json.JSONDecodeError, AttributeError):
        return None
    if not isinstance(payload, dict) or payload.get("type") not in RELAYED_SIGNAL_TYPES:
        return None
    return {"type": payload["type"], "data": payload.get("data", {})}


async def signals_relay():
    """Forward agent updates published on the Redis 'signals' channel to WebSocket clients"""
    while True:
        pubsub = None
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe("signals")
            async for msg in pubsub.listen():
                if msg.get("type") != "message":
                    continue
                out = relay_message(msg["data"])
                if out:
                    await manager.broadcast(out)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"Signals relay error: {e}")
            await asyncio.sleep(5)
        finally:
            if pubsub is not None:
                await pubsub.aclose()
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
    async with _db().db.session() as session:
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
        
        # the lists hold ORM rows, which the nested models only accept when read by attribute
        return SystemOverview.model_validate({
            "metrics": metrics,
            "agents": agents,
            "recent_trades": recent_trades,
            "open_positions": open_positions,
            "recent_signals": recent_signals,
            "equity_curve": equity_curve,
        }, from_attributes=True)


# --- Trades ---
@app.get("/api/trades", response_model=List[TradeResponse])
async def get_trades(
    symbol: Optional[str] = None,
    strategy: Optional[str] = None,
    limit: int = Query(100, le=1000),
    days: int = Query(30, le=365)
):
    async with _db().db.session() as session:
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
    async with _db().db.session() as session:
        from hybrid.storage import Trade
        trade = await session.get(Trade, trade_id)
        if not trade:
            raise HTTPException(404, "Trade not found")
        return trade


# --- Positions ---
@app.get("/api/positions", response_model=List[PositionResponse])
async def get_positions(open_only: bool = True):
    async with _db().db.session() as session:
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
    async with _db().db.session() as session:
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
# Must precede /api/equity/{account}, which would otherwise capture "history".
@app.get("/api/equity/history")
async def get_equity_history(days: int = Query(30, le=365), account: str = "default"):
    async with _db().db.session() as session:
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


@app.get("/api/equity/{account}", response_model=List[EquityPoint])
async def get_equity(account: str, days: int = Query(30, le=365)):
    async with _db().db.session() as session:
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
    async with _db().db.session() as session:
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
    async with _db().db.session() as session:
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
    data = await _redis().hgetall(f"dashboard:{channel}")
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
        async with _db().db.session() as session:
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
            
            async with _db().db.session() as session:
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

# Background tasks are started in lifespan()


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
# --- Risk Metrics ---
@app.get("/api/risk")
async def get_risk_metrics():
    """Get live risk metrics"""
    if not storage:
        return {"error": "Storage not available"}
    try:
        async with _db().db.session() as session:
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


# --- Decision band: latest quant score per coin against the thresholds ---
@app.get("/api/decision-band")
async def decision_band():
    from sqlalchemy import select
    cfg = HybridConfig()
    since = datetime.now(timezone.utc) - timedelta(days=1)
    async with _db().db.session() as session:
        result = await session.execute(
            select(Signal).where(Signal.agent == "quant_agent", Signal.timestamp >= since)
            .order_by(Signal.timestamp.desc()))
        latest: Dict[str, Signal] = {}
        for sig in result.scalars():
            latest.setdefault(sig.symbol, sig)

    async def status(key: str) -> Optional[str]:
        try:
            value = await _redis().get(key)
        except Exception:
            return None
        return value.decode() if isinstance(value, bytes) else value

    last = await status("system:last_analysis")
    interval = await status("system:analysis_interval")
    return {
        "sell_threshold": cfg.sell_threshold,
        "buy_threshold": cfg.buy_threshold,
        "mode": await status("system:mode") or "unknown",
        "last_analysis": float(last) if last else None,
        "next_analysis": float(last) + float(interval) if last and interval else None,
        "coins": [
            {"symbol": s.symbol, "score": float(s.strength or 0.5), "action": s.action or "hold",
             "confidence": float(s.confidence or 0), "at": s.timestamp.isoformat()}
            for s in sorted(latest.values(), key=lambda s: s.symbol)
        ],
    }


# --- Stocks: separate research-only ledger (not trade signals) ---
@app.get("/api/stocks/shadow")
async def stock_shadow_state():
    from sqlalchemy import select
    from hybrid.stock_shadow import SYMBOLS, enabled
    from hybrid.storage import StockShadowDecision

    async with _db().db.session() as session:
        result = await session.execute(select(StockShadowDecision)
                                       .order_by(StockShadowDecision.session_date.desc()).limit(60))
        latest = {}
        for row in result.scalars():
            latest.setdefault(row.symbol, row)
    config = HybridConfig()
    return {"mode": "shadow", "enabled": enabled(), "symbols": list(SYMBOLS),
            "buy_threshold": config.buy_threshold, "sell_threshold": config.sell_threshold,
            "orders_enabled": False, "source": "technical-only, completed daily bars",
            "decisions": [{"symbol": r.symbol, "session_date": r.session_date.isoformat(),
                           "session_close": r.session_close.isoformat(),
                           "analyzed_at": r.analyzed_at.isoformat(), "action": r.action,
                           "score": float(r.score), "confidence": float(r.confidence),
                           "close_price": float(r.close_price)} for r in latest.values()]}


# --- Learning loop: outcomes, re-fit proposals, approval ---
def _proposal_json(p) -> dict:
    return {"id": p.id, "status": p.status, "created_at": p.created_at.isoformat(),
            "decided_at": p.decided_at.isoformat() if p.decided_at else None,
            "current": p.current, "proposed": p.proposed, "metrics": p.metrics}


@app.get("/api/learning")
async def learning_state():
    from hybrid.learning import MIN_SAMPLES, daily_samples
    repo = _db().learning
    proposals = await repo.list_proposals(20)
    approved = next((p for p in proposals if p.status == "approved"), None)
    active = await repo.active_weights()
    return {
        "active": {"weights": active, "source": "approved",
                   "since": approved.decided_at.isoformat() if approved and approved.decided_at else None}
        if active else {"weights": HybridConfig().tech_weights(), "source": "starting", "since": None},
        "progress": {"labelled_days": len(daily_samples(await repo.labelled_samples())), "needed": MIN_SAMPLES},
        "pending": next((_proposal_json(p) for p in proposals if p.status == "pending"), None),
        "history": [_proposal_json(p) for p in proposals if p.status != "pending"][:10],
    }


async def _decide(proposal_id: str, status: str) -> dict:
    repo = _db().learning
    proposal = await repo.get_proposal(proposal_id)
    if proposal is None:
        raise HTTPException(status_code=404, detail="Proposal not found")
    if proposal.status != "pending":
        raise HTTPException(status_code=409, detail=f"Proposal is already {proposal.status}")
    await repo.set_status(proposal_id, status)
    return {"id": proposal_id, "status": status}


@app.post("/api/learning/proposals/{proposal_id}/approve")
async def approve_proposal(proposal_id: str):
    return await _decide(proposal_id, "approved")


@app.post("/api/learning/proposals/{proposal_id}/reject")
async def reject_proposal(proposal_id: str):
    return await _decide(proposal_id, "rejected")


# --- LLM Signals panel: recorded analyses, human labels, extractor accuracy ---
SignalName = Literal["sentiment", "fundamental", "news", "research_debate", "trader_action", "portfolio_decision"]


class SignalLabelBody(BaseModel):
    label: Literal["bearish", "neutral", "bullish"]


def _analysis_summary(analysis, labelled: int) -> dict:
    return {
        "id": analysis.id,
        "ticker": analysis.ticker,
        "trade_date": analysis.trade_date,
        "created_at": analysis.created_at.isoformat(),
        "labelled": labelled,
        "total": sum(1 for text in analysis.reports.values() if text),  # reports that exist to label
    }


@app.get("/api/llm/analyses")
async def list_llm_analyses(limit: int = Query(50, ge=1, le=200)):
    return [_analysis_summary(a, n) for a, n in await _db().llm.list_recent(limit)]


@app.get("/api/llm/analyses/{analysis_id}")
async def get_llm_analysis(analysis_id: str):
    from hybrid.signal_eval import direction
    from hybrid.signal_extractor import SIGNALS
    analysis, labels = await _db().llm.get(analysis_id)
    if analysis is None:
        raise HTTPException(status_code=404, detail="Analysis not found")
    cfg = HybridConfig()
    # share of the hybrid decision each signal carries; trader/portfolio aren't weighted
    weights = {"sentiment": cfg.weight_sentiment, "fundamental": cfg.weight_fundamental,
               "news": cfg.weight_news, "research_debate": cfg.weight_research_debate}
    return {
        **_analysis_summary(analysis, len(labels)),
        "signals": [
            {
                "signal": name,
                "report": analysis.reports.get(name, ""),
                "score": analysis.scores.get(name, 0.5),
                "direction": direction(analysis.scores.get(name, 0.5)),
                "source": analysis.sources.get(name, "unavailable"),
                "confidence": analysis.confidences.get(name),
                "label": labels.get(name),
                "weight": weights.get(name),
            }
            for name in SIGNALS
        ],
    }


@app.put("/api/llm/analyses/{analysis_id}/labels/{signal}")
async def set_signal_label(analysis_id: str, signal: SignalName, body: SignalLabelBody):
    analysis, _ = await _db().llm.get(analysis_id)
    if analysis is None:
        raise HTTPException(status_code=404, detail="Analysis not found")
    await _db().llm.set_label(analysis_id, signal, body.label)
    return {"signal": signal, "label": body.label}


@app.delete("/api/llm/analyses/{analysis_id}/labels/{signal}", status_code=204)
async def clear_signal_label(analysis_id: str, signal: SignalName):
    await _db().llm.clear_label(analysis_id, signal)
    return Response(status_code=204)


@app.get("/api/llm/accuracy")
async def llm_accuracy():
    from hybrid.signal_eval import accuracy
    return accuracy(await _db().llm.labelled_rows())


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
        async with _db().db.session() as session:
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
                running_equity += float(trade.pnl or 0)
                equity_points.append({
                    "timestamp": trade.timestamp.isoformat(),
                    "equity": running_equity,
                    "trade_pnl": float(trade.pnl or 0)
                })
            
            return {
                "strategy_id": strategy_id,
                "days": days,
                "total_trades": len(trades),
                "total_pnl": sum(float(t.pnl or 0) for t in trades),
                "equity_curve": equity_points,
                "trades": [
                    {
                        "timestamp": t.timestamp.isoformat(),
                        "symbol": t.symbol,
                        "side": t.side,
                        "volume": float(t.volume),
                        "price": float(t.price),
                        "pnl": float(t.pnl or 0)
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
