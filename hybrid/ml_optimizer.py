import json
import logging
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf
from scipy.optimize import minimize

warnings.filterwarnings("ignore")
logger = logging.getLogger("ML_Optimizer")
logging.basicConfig(level=logging.INFO, format="%(message)s")

# Setup paths
HYBRID_DIR = Path(__file__).parent
WEIGHTS_FILE = HYBRID_DIR / "learned_weights.json"

def compute_vectorized_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Computes technical scores (0 to 1) fully vectorized for the ML backtest."""
    df = df.copy()
    
    # 1. RSI (14-day)
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    # RSI Score: <30 is 1.0 (Buy), >70 is 0.0 (Sell)
    df['score_rsi'] = 1.0 - (rsi / 100.0)
    
    # 2. EMA Crossover
    ema12 = df['Close'].ewm(span=12, adjust=False).mean()
    ema26 = df['Close'].ewm(span=26, adjust=False).mean()
    df['score_ema'] = np.where(ema12 > ema26, 0.7, 0.3)
    
    # 3. Bollinger Bands
    sma20 = df['Close'].rolling(window=20).mean()
    std20 = df['Close'].rolling(window=20).std()
    upper = sma20 + (2 * std20)
    lower = sma20 - (2 * std20)
    # 0 = at upper band, 1 = at lower band
    pos = (upper - df['Close']) / (upper - lower)
    df['score_bollinger'] = np.clip(pos, 0, 1)
    
    # 4. Volume
    sma_vol = df['Volume'].rolling(window=20).mean()
    vol_ratio = df['Volume'] / sma_vol
    df['score_volume'] = np.clip(vol_ratio / 2.0, 0, 1) * 0.5 + 0.25 # Center around 0.5
    
    # Forward returns (what happens the NEXT day)
    df['fwd_return'] = df['Close'].pct_change().shift(-1)
    
    return df.dropna()

def prepare_backtest_data(tickers=["AAPL", "NVDA", "MSFT", "SPY"]):
    """Downloads historical data and computes indicators for the ML universe."""
    logger.info(f"Downloading 2 years of training data for {tickers}...")
    dfs = []
    for ticker in tickers:
        df = yf.download(ticker, period="2y", interval="1d", progress=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(1)
        if len(df) > 100:
            df = compute_vectorized_indicators(df)
            dfs.append(df)
    return pd.concat(dfs, axis=0)

def objective_function(weights, df):
    """
    Calculates the negative Sharpe Ratio of the strategy using the given weights.
    We return negative Sharpe because scipy.optimize *minimizes* the objective.
    """
    # Normalize weights so they always sum to 1.0
    w_sum = np.sum(weights)
    if w_sum == 0:
        return 0.0
    weights = weights / w_sum
    w_rsi, w_ema, w_boll, w_vol = weights
    
    # Compute daily composite score
    composite = (
        df['score_rsi'] * w_rsi +
        df['score_ema'] * w_ema +
        df['score_bollinger'] * w_boll +
        df['score_volume'] * w_vol
    )
    
    # Simple trading logic: Buy if > 0.60, Short if < 0.40, else Flat
    positions = np.where(composite > 0.60, 1.0, np.where(composite < 0.40, -1.0, 0.0))
    
    # Calculate returns
    strategy_returns = positions * df['fwd_return']
    
    mean_ret = np.mean(strategy_returns)
    std_ret = np.std(strategy_returns)
    
    if std_ret == 0:
        return 0.0 # Bad weights
        
    # Annualized Sharpe Ratio
    sharpe = (mean_ret / std_ret) * np.sqrt(252)
    
    return -sharpe # Minimize negative sharpe = Maximize positive sharpe

def optimize_weights():
    """Runs the Machine Learning optimization loop."""
    logger.info("🧠 Initializing ML Weight Optimizer...")
    df = prepare_backtest_data()
    
    # Initial guess (equal weighting)
    init_weights = [0.25, 0.25, 0.25, 0.25]
    
    # Constraints: Weights must sum to 1.0
    constraints = ({'type': 'eq', 'fun': lambda w: np.sum(w) - 1.0})
    
    # Bounds: No weight can be less than 0 or greater than 1
    bounds = tuple((0.0, 1.0) for _ in range(4))
    
    from scipy.optimize import differential_evolution
    
    logger.info("⚙️ Running Differential Evolution (Genetic Algorithm) Optimization...")
    
    # Run Differential Evolution
    result = differential_evolution(
        objective_function,
        bounds=bounds,
        args=(df,),
        strategy='best1bin',
        maxiter=100,
        popsize=15,
        tol=0.01,
        mutation=(0.5, 1.0),
        recombination=0.7,
        seed=42
    )
    
    if result.success:
        # DE doesn't enforce sum=1.0 easily in bounds, so we normalize the result
        opt_w = result.x / np.sum(result.x)
        logger.info("\n✅ Optimization Complete!")
        logger.info(f"Optimal Sharpe Ratio achieved: {-result.fun:.2f}")
        logger.info("--- New Learned Technical Weights ---")
        logger.info(f"RSI Weight:        {opt_w[0]:.4f}")
        logger.info(f"EMA Weight:        {opt_w[1]:.4f}")
        logger.info(f"Bollinger Weight:  {opt_w[2]:.4f}")
        logger.info(f"Volume Weight:     {opt_w[3]:.4f}")
        
        # Save to JSON
        learned_config = {
            "tech_weight_rsi": float(opt_w[0]),
            "tech_weight_ema": float(opt_w[1]),
            "tech_weight_bollinger": float(opt_w[2]),
            "tech_weight_volume": float(opt_w[3])
        }
        
        with open(WEIGHTS_FILE, "w") as f:
            json.dump(learned_config, f, indent=4)
            
        logger.info(f"\n💾 Saved learned weights to {WEIGHTS_FILE.name}")
        logger.info("The hybrid/config.py will now automatically load these weights.")
    else:
        logger.error("❌ Optimization failed to converge.")

if __name__ == "__main__":
    optimize_weights()
