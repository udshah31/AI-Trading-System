import pandas as pd
import numpy as np
import yfinance as yf
from stable_baselines3 import PPO
from stable_baselines3.common.vec_env import DummyVecEnv, VecNormalize
from stable_baselines3.common.callbacks import EvalCallback, CheckpointCallback
from stable_baselines3.common.monitor import Monitor
import gymnasium as gym
from gymnasium import spaces
from pathlib import Path
import json


class TradingEnv(gym.Env):
    """
    Trading Environment for RL Agent
    
    State (12 features):
    - Technical: RSI, EMA_spread, BB_position, Volume_ratio, ATR_pct
    - LLM: sentiment, fundamental, news, debate, trader, portfolio
    - Portfolio: position_pct, drawdown_pct
    
    Action: Continuous [-1, 1] position target
    Reward: Risk-adjusted returns (Sharpe-like)
    """
    
    def __init__(self, df: pd.DataFrame, llm_signals: dict = None, 
                 initial_capital: float = 100_000, transaction_cost: float = 0.001,
                 max_position: float = 1.0, risk_penalty: float = 0.1):
        super().__init__()
        
        self.df = df.reset_index(drop=True)
        self.llm_signals = llm_signals or self._default_llm_signals()
        self.initial_capital = initial_capital
        self.transaction_cost = transaction_cost
        self.max_position = max_position
        self.risk_penalty = risk_penalty
        
        # Precompute technical features
        self._compute_features()
        
        # 12-dimensional observation space
        self.observation_space = spaces.Box(
            low=-np.inf, high=np.inf, shape=(12,), dtype=np.float32
        )
        # Continuous action: target position [-1, 1]
        self.action_space = spaces.Box(
            low=-1.0, high=1.0, shape=(1,), dtype=np.float32
        )
        
        self.current_step = 0
        self.position = 0.0
        self.capital = initial_capital
        self.peak_capital = initial_capital
        self.trades = []
    
    def _default_llm_signals(self):
        return {col: 0.5 for col in [
            'sentiment_score', 'fundamental_score', 'news_score',
            'research_debate_score', 'trader_action_score', 'portfolio_decision_score'
        ]}
    
    def _compute_features(self):
        """Vectorized technical indicator computation"""
        df = self.df.copy()
        
        # RSI
        delta = df['Close'].diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss.replace(0, np.inf)
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # EMA
        df['ema_fast'] = df['Close'].ewm(span=12, adjust=False).mean()
        df['ema_slow'] = df['Close'].ewm(span=26, adjust=False).mean()
        
        # Bollinger Bands
        sma20 = df['Close'].rolling(20).mean()
        std20 = df['Close'].rolling(20).std()
        df['bb_upper'] = sma20 + 2 * std20
        df['bb_lower'] = sma20 - 2 * std20
        df['bb_mid'] = sma20
        
        # ATR
        high_low = df['High'] - df['Low']
        high_close = (df['High'] - df['Close'].shift()).abs()
        low_close = (df['Low'] - df['Close'].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        df['atr'] = tr.rolling(14).mean()
        
        # Volume
        df['volume_sma'] = df['Volume'].rolling(20).mean()
        
        # Normalized features
        self.df['rsi_norm'] = self.df['rsi'] / 100.0
        self.df['ema_spread'] = (self.df['ema_fast'] - self.df['ema_slow']) / self.df['ema_slow']
        bb_range = self.df['bb_upper'] - self.df['bb_lower']
        self.df['bb_position'] = np.where(
            bb_range > 0,
            (self.df['Close'] - self.df['bb_lower']) / bb_range,
            0.5
        )
        self.df['volume_ratio'] = np.where(
            self.df['volume_sma'] > 0,
            self.df['Volume'] / self.df['volume_sma'],
            1.0
        )
        self.df['atr_pct'] = self.df['atr'] / self.df['Close']
        
        # Forward returns for reward calculation
        self.df['fwd_return'] = self.df['Close'].pct_change().shift(-1)
        
        # Drop NaN
        self.df = self.df.dropna().reset_index(drop=True)
    
    def reset(self, seed=None, options=None):
        super().reset(seed=seed)
        self.current_step = 50  # Start after indicators stabilize
        self.position = 0.0
        self.capital = self.initial_capital
        self.peak_capital = self.initial_capital
        self.trades = []
        return self._get_obs(), {}
    
    def step(self, action):
        target_position = np.clip(float(action[0]), -self.max_position, self.max_position)
        trade_size = target_position - self.position
        
        # Transaction cost
        cost = abs(trade_size) * self.capital * self.transaction_cost
        self.capital -= cost
        
        # Execute trade
        self.position = target_position
        
        # Calculate PnL
        if self.current_step < len(self.df) - 1:
            price_return = self.df['fwd_return'].iloc[self.current_step]
            pnl = self.position * price_return * self.capital
            self.capital += pnl
            
            # Track drawdown
            if self.capital > self.peak_capital:
                self.peak_capital = self.capital
            drawdown = (self.peak_capital - self.capital) / self.peak_capital
        else:
            drawdown = 0
        
        # Record trade
        self.trades.append({
            'step': self.current_step,
            'position': self.position,
            'capital': self.capital,
            'drawdown': drawdown,
            'cost': cost
        })
        
        # Reward: Risk-adjusted return
        # Sharpe-like: return / (volatility + drawdown_penalty)
        if len(self.trades) > 10:
            recent_returns = [t['capital'] / self.trades[i-1]['capital'] - 1 
                            for i, t in enumerate(self.trades) if i > 0]
            vol = np.std(recent_returns) if len(recent_returns) > 1 else 0.01
            avg_return = np.mean(recent_returns) if recent_returns else 0
            sharpe = avg_return / (vol + 1e-6) * np.sqrt(252)
        else:
            sharpe = 0
        
        # Penalize large drawdowns and excessive position
        drawdown_penalty = drawdown * 10
        position_penalty = max(0, abs(self.position) - 0.5) * self.risk_penalty
        
        reward = sharpe * 0.01 - drawdown_penalty - position_penalty - cost / self.capital * 100
        
        self.current_step += 1
        terminated = self.current_step >= len(self.df) - 1
        truncated = False
        
        return self._get_obs(), reward, terminated, truncated, {
            'capital': self.capital,
            'position': self.position,
            'drawdown': drawdown,
            'sharpe': sharpe
        }
    
    def _get_obs(self):
        row = self.df.iloc[self.current_step]
        
        # Technical features (5)
        tech = [
            row['rsi_norm'],
            np.clip(row['ema_spread'] * 10, -1, 1),
            row['bb_position'],
            np.clip(row['volume_ratio'] / 3, 0, 1),
            np.clip(row['atr_pct'] * 100, 0, 1)
        ]
        
        # LLM signals (6) - would come from message bus in production
        llm = [
            self.llm_signals.get('sentiment_score', 0.5),
            self.llm_signals.get('fundamental_score', 0.5),
            self.llm_signals.get('news_score', 0.5),
            self.llm_signals.get('research_debate_score', 0.5),
            self.llm_signals.get('trader_action_score', 0.5),
            self.llm_signals.get('portfolio_decision_score', 0.5)
        ]
        
        # Portfolio state (2)
        portfolio = [
            self.position,
            (self.peak_capital - self.capital) / self.peak_capital if self.peak_capital > 0 else 0
        ]
        
        return np.array(tech + llm + portfolio, dtype=np.float32)


def prepare_training_data(tickers=["BTC-USD", "ETH-USD", "SOL-USD"], period="2y"):
    """Download and prepare multi-asset training data"""
    all_data = []
    
    for ticker in tickers:
        print(f"Downloading {ticker}...")
        df = yf.download(ticker, period=period, interval="1d", progress=False, auto_adjust=True)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        
        if len(df) > 200:
            df['symbol'] = ticker
            all_data.append(df)
    
    combined = pd.concat(all_data, axis=0)
    print(f"Total training samples: {len(combined)}")
    return combined


def train_rl_model(
    tickers=["BTC-USD", "ETH-USD", "SOL-USD"],
    period="2y",
    timesteps=500_000,
    model_path="models/ppo_trader",
    n_envs=4
):
    """Train PPO model with vectorized environments"""
    
    # Prepare data
    data = prepare_training_data(tickers, period)
    
    # Create vectorized env
    def make_env():
        def _init():
            env = TradingEnv(data.sample(frac=1).reset_index(drop=True))  # Shuffle
            env = Monitor(env)
            return env
        return _init
    
    vec_env = DummyVecEnv([make_env() for _ in range(n_envs)])
    vec_env = VecNormalize(vec_env, norm_obs=True, norm_reward=True, clip_obs=10.0)
    
    # Callbacks
    eval_callback = EvalCallback(
        vec_env,
        best_model_save_path=f"{model_path}_best",
        log_path=f"{model_path}_logs",
        eval_freq=10_000,
        deterministic=True,
        render=False
    )
    
    checkpoint_callback = CheckpointCallback(
        save_freq=50_000,
        save_path=f"{model_path}_checkpoints",
        name_prefix="ppo_trader"
    )
    
    # Model
    model = PPO(
        "MlpPolicy",
        vec_env,
        learning_rate=3e-4,
        n_steps=2048,
        batch_size=64,
        n_epochs=10,
        gamma=0.99,
        gae_lambda=0.95,
        clip_range=0.2,
        ent_coef=0.01,
        vf_coef=0.5,
        max_grad_norm=0.5,
        verbose=1,
        tensorboard_log=f"{model_path}_tb",
        device="auto"
    )
    
    print(f"Training for {timesteps} timesteps...")
    model.learn(
        total_timesteps=timesteps,
        callback=[eval_callback, checkpoint_callback],
        progress_bar=True
    )
    
    # Save final model and normalization stats
    model.save(model_path)
    vec_env.save(f"{model_path}_vecnormalize.pkl")
    
    print(f"Model saved to {model_path}")
    return model, vec_env


def backtest_model(model_path="models/ppo_trader", test_tickers=["BTC-USD"], period="6mo"):
    """Backtest trained model on out-of-sample data"""
    from stable_baselines3.common.vec_env import VecNormalize
    
    # Load model and normalization
    model = PPO.load(model_path)
    vec_norm = VecNormalize.load(f"{model_path}_vecnormalize.pkl", DummyVecEnv([lambda: None]))
    vec_norm.training = False
    vec_norm.norm_reward = False
    
    # Get test data
    test_data = prepare_training_data(test_tickers, period)
    
    # Run backtest
    env = TradingEnv(test_data)
    obs, _ = env.reset()
    done = False
    
    equity_curve = [env.initial_capital]
    positions = []
    
    while not done:
        obs_norm = vec_norm.normalize_obs(obs)
        action, _ = model.predict(obs_norm, deterministic=True)
        obs, reward, terminated, truncated, info = env.step(action)
        done = terminated or truncated
        
        equity_curve.append(info['capital'])
        positions.append(info['position'])
    
    # Metrics
    equity = np.array(equity_curve)
    returns = np.diff(equity) / equity[:-1]
    
    total_return = (equity[-1] - equity[0]) / equity[0]
    sharpe = np.mean(returns) / (np.std(returns) + 1e-6) * np.sqrt(252)
    max_dd = np.max((np.maximum.accumulate(equity) - equity) / np.maximum.accumulate(equity))
    win_rate = np.mean(returns > 0)
    
    print(f"\n{'='*50}")
    print(f"BACKTEST RESULTS ({test_tickers[0]})")
    print(f"{'='*50}")
    print(f"Total Return:     {total_return:.2%}")
    print(f"Sharpe Ratio:     {sharpe:.2f}")
    print(f"Max Drawdown:     {max_dd:.2%}")
    print(f"Win Rate:         {win_rate:.2%}")
    print(f"Final Equity:     ${equity[-1]:,.2f}")
    print(f"{'='*50}")
    
    return {
        'equity_curve': equity_curve,
        'positions': positions,
        'returns': returns,
        'metrics': {
            'total_return': total_return,
            'sharpe': sharpe,
            'max_drawdown': max_dd,
            'win_rate': win_rate
        }
    }


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--train", action="store_true", help="Train new model")
    parser.add_argument("--backtest", action="store_true", help="Run backtest")
    parser.add_argument("--tickers", nargs="+", default=["BTC-USD", "ETH-USD", "SOL-USD"])
    parser.add_argument("--timesteps", type=int, default=500_000)
    args = parser.parse_args()
    
    Path("models").mkdir(exist_ok=True)
    
    if args.train:
        train_rl_model(tickers=args.tickers, timesteps=args.timesteps)
    
    if args.backtest:
        backtest_model(test_tickers=args.tickers)
