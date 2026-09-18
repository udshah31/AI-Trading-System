"""
Prometheus Metrics for Trading System
"""
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, generate_latest
from functools import wraps
import time
import asyncio

# Create custom registry
registry = CollectorRegistry()

# =============================================================================
# TRADING METRICS
# =============================================================================

TRADES_TOTAL = Counter(
    'trades_total', 'Total trades executed',
    ['symbol', 'side', 'exchange', 'status', 'strategy'],
    registry=registry
)

TRADE_VOLUME_USD = Histogram(
    'trade_volume_usd', 'Trade volume in USD',
    ['symbol', 'exchange', 'strategy'],
    buckets=[10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000],
    registry=registry
)

TRADE_PNL_USD = Histogram(
    'trade_pnl_usd', 'Trade PnL in USD',
    ['symbol', 'strategy'],
    buckets=[-10000, -5000, -1000, -500, -100, -50, -10, 0, 10, 50, 100, 500, 1000, 5000, 10000],
    registry=registry
)

TRADE_LATENCY = Histogram(
    'trade_execution_latency_seconds', 'Order execution latency',
    ['exchange', 'order_type'],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0],
    registry=registry
)

POSITION_SIZE_USD = Gauge(
    'position_size_usd', 'Current position size in USD',
    ['symbol', 'strategy', 'account'],
    registry=registry
)

POSITION_PNL_USD = Gauge(
    'position_pnl_usd', 'Unrealized PnL in USD',
    ['symbol', 'strategy', 'account'],
    registry=registry
)

EQUITY_USD = Gauge(
    'equity_usd', 'Current equity in USD',
    ['account', 'currency'],
    registry=registry
)

DRAWDOWN_PCT = Gauge(
    'drawdown_pct', 'Current drawdown percentage',
    ['account'],
    registry=registry

)

DAILY_PNL_USD = Gauge(
    'daily_pnl_usd', 'Daily realized PnL',
    ['account', 'strategy'],
    registry=registry
)

WIN_RATE = Gauge(
    'win_rate', 'Win rate (0-1)',
    ['strategy', 'timeframe'],
    registry=registry
)

SHARPE_RATIO = Gauge(
    'sharpe_ratio', 'Rolling Sharpe ratio',
    ['strategy', 'window'],
    registry=registry
)

# =============================================================================
# AGENT METRICS
# =============================================================================

AGENT_HEARTBEAT = Gauge(
    'agent_heartbeat_timestamp', 'Agent last heartbeat unix timestamp',
    ['agent', 'host'],
    registry=registry
)

AGENT_UPTIME = Gauge(
    'agent_uptime_seconds', 'Agent uptime in seconds',
    ['agent'],
    registry=registry
)

SIGNAL_PROCESSING_LATENCY = Histogram(
    'signal_processing_latency_seconds', 'Signal processing latency',
    ['agent', 'signal_type'],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
    registry=registry
)

SIGNALS_GENERATED = Counter(
    'signals_generated_total', 'Total signals generated',
    ['agent', 'signal_type', 'action'],
    registry=registry
)

MESSAGES_PROCESSED = Counter(
    'messages_processed_total', 'Total messages processed from bus',
    ['agent', 'channel', 'status'],
    registry=registry
)

MESSAGE_QUEUE_LAG = Gauge(
    'message_queue_lag_seconds', 'Message processing lag',
    ['agent', 'channel'],
    registry=registry
)

ERRORS_TOTAL = Counter(
    'errors_total', 'Total errors',
    ['agent', 'error_type', 'severity'],
    registry=registry
)

# =============================================================================
# MARKET DATA METRICS
# =============================================================================

MARKET_DATA_LATENCY = Histogram(
    'market_data_latency_seconds', 'Market data fetch latency',
    ['source', 'symbol', 'endpoint'],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0],
    registry=registry
)

MARKET_DATA_STALENESS = Gauge(
    'market_data_staleness_seconds', 'Age of latest market data',
    ['source', 'symbol'],
    registry=registry
)

ORDERBOOK_DEPTH = Gauge(
    'orderbook_depth_levels', 'Orderbook depth levels available',
    ['exchange', 'symbol', 'side'],
    registry=registry
)

ORDERBOOK_SPREAD_BPS = Gauge(
    'orderbook_spread_bps', 'Orderbook spread in basis points',
    ['exchange', 'symbol'],
    registry=registry
)

WEBSOCKET_RECONNECTS = Counter(
    'websocket_reconnects_total', 'WebSocket reconnection count',
    ['exchange', 'channel'],
    registry=registry
)

WEBSOCKET_MESSAGES = Counter(
    'websocket_messages_total', 'WebSocket messages received',
    ['exchange', 'channel', 'message_type'],
    registry=registry
)

# =============================================================================
# RISK METRICS
# =============================================================================

RISK_CHECK_LATENCY = Histogram(
    'risk_check_latency_seconds', 'Risk check processing time',
    ['check_type'],
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5],
    registry=registry
)

RISK_REJECTIONS = Counter(
    'risk_rejections_total', 'Orders rejected by risk manager',
    ['reason', 'symbol'],
    registry=registry
)

PORTFOLIO_VAR = Gauge(
    'portfolio_var_95', 'Portfolio Value at Risk (95%)',
    ['account', 'horizon'],
    registry=registry
)

MARGIN_USAGE_PCT = Gauge(
    'margin_usage_pct', 'Margin usage percentage',
    ['account'],
    registry=registry
)

# =============================================================================
# RL METRICS
# =============================================================================

RL_EPISODE_REWARD = Histogram(
    'rl_episode_reward', 'RL episode total reward',
    ['model', 'env'],
    registry=registry
)

RL_EPISODE_LENGTH = Histogram(
    'rl_episode_length', 'RL episode length',
    ['model', 'env'],
    registry=registry
)

RL_ACTION_DISTRIBUTION = Histogram(
    'rl_action_distribution', 'RL action distribution',
    ['model', 'action_dim'],
    registry=registry
)

RL_TRAINING_LOSS = Gauge(
    'rl_training_loss', 'Current training loss',
    ['model', 'loss_type'],
    registry=registry
)

RL_LEARNING_RATE = Gauge(
    'rl_learning_rate', 'Current learning rate',
    ['model'],
    registry=registry
)

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def track_latency(metric: Histogram, labels: dict = None):
    """Decorator to track function latency"""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return await func(*args, **kwargs)
            finally:
                duration = time.perf_counter() - start
                metric.labels(**labels).observe(duration) if labels else metric.observe(duration)
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                duration = time.perf_counter() - start
                metric.labels(**labels).observe(duration) if labels else metric.observe(duration)
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


def record_trade(symbol: str, side: str, exchange: str, status: str, 
                 strategy: str, volume_usd: float, pnl_usd: float = 0):
    """Record a trade execution"""
    TRADES_TOTAL.labels(
        symbol=symbol, side=side, exchange=exchange, 
        status=status, strategy=strategy
    ).inc()
    TRADE_VOLUME_USD.labels(
        symbol=symbol, exchange=exchange, strategy=strategy
    ).observe(volume_usd)
    if pnl_usd != 0:
        TRADE_PNL_USD.labels(symbol=symbol, strategy=strategy).observe(pnl_usd)


def record_signal(agent: str, signal_type: str, action: str):
    """Record a signal generation"""
    SIGNALS_GENERATED.labels(
        agent=agent, signal_type=signal_type, action=action
    ).inc()


def record_error(agent: str, error_type: str, severity: str = "error"):
    """Record an error"""
    ERRORS_TOTAL.labels(agent=agent, error_type=error_type, severity=severity).inc()


def update_heartbeat(agent: str, host: str = "localhost"):
    """Update agent heartbeat"""
    AGENT_HEARTBEAT.labels(agent=agent, host=host).set(time.time())


def get_metrics() -> bytes:
    """Get Prometheus metrics output"""
    return generate_latest(registry)


class MetricsMiddleware:
    """FastAPI middleware for HTTP metrics"""
    
    def __init__(self, app):
        self.app = app
    
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        
        start = time.perf_counter()
        
        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                duration = time.perf_counter() - start
                # Could add HTTP metrics here
            await send(message)
        
        await self.app(scope, receive, send_wrapper)
