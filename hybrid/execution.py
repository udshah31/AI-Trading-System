"""
Alpaca Paper Trading Execution
================================
Submits approved trades to Alpaca's paper trading API.

SAFETY: This module ONLY works with paper trading accounts.
It will refuse to execute if paper trading mode is not enabled.
"""

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from hybrid.config import HybridConfig
from hybrid.risk_manager import RiskAssessment


@dataclass
class ExecutionResult:
    """Result of a trade execution attempt."""

    success: bool = False
    order_id: Optional[str] = None
    ticker: str = ""
    action: str = ""
    shares: float = 0.0
    price: float = 0.0
    message: str = ""
    timestamp: str = ""

    def summary(self) -> str:
        """Human-readable execution result."""
        status = "✅ EXECUTED" if self.success else "❌ FAILED"
        return (
            f"{'═' * 50}\n"
            f"  EXECUTION: {status}\n"
            f"{'═' * 50}\n"
            f"  Ticker:    {self.ticker}\n"
            f"  Action:    {self.action}\n"
            f"  Shares:    {self.shares:.4f}\n"
            f"  Price:     ${self.price:.2f}\n"
            f"  Order ID:  {self.order_id or 'N/A'}\n"
            f"  Message:   {self.message}\n"
            f"  Time:      {self.timestamp}\n"
            f"{'═' * 50}\n"
        )


class AlpacaExecutor:
    """Executes paper trades via the Alpaca API.

    This class provides both a LIVE mode (using alpaca-py SDK)
    and a DRY RUN mode (logs the trade without connecting to Alpaca).
    """

    def __init__(self, config: HybridConfig, dry_run: bool = True):
        """Initialize the Alpaca executor.

        Args:
            config: Hybrid system configuration.
            dry_run: If True, only logs trades without actually submitting.
                     Set to False to connect to Alpaca paper trading API.
        """
        # SAFETY: Never allow live trading
        if not config.alpaca_paper_trade:
            raise ValueError(
                "SAFETY BLOCK: alpaca_paper_trade must be True. "
                "This system is for PAPER TRADING ONLY."
            )

        self.config = config
        self.dry_run = dry_run
        self._client = None

        if not dry_run:
            self._init_alpaca_client()

    def _init_alpaca_client(self) -> None:
        """Initialize the Alpaca API client."""
        api_key = self.config.alpaca_api_key or os.environ.get("ALPACA_API_KEY")
        secret_key = self.config.alpaca_secret_key or os.environ.get(
            "ALPACA_SECRET_KEY"
        )

        if not api_key or not secret_key:
            print(
                "[AlpacaExecutor] WARNING: Alpaca API keys not configured. "
                "Falling back to dry-run mode."
            )
            self.dry_run = True
            return

        try:
            from alpaca.trading.client import TradingClient

            self._client = TradingClient(
                api_key=api_key,
                secret_key=secret_key,
                paper=True,  # ALWAYS paper trading
            )
            print("[AlpacaExecutor] Connected to Alpaca Paper Trading API.")
        except ImportError:
            print(
                "[AlpacaExecutor] alpaca-py not installed. "
                "Run: pip install alpaca-py. Falling back to dry-run mode."
            )
            self.dry_run = True
        except Exception as e:
            print(f"[AlpacaExecutor] Failed to connect to Alpaca: {e}")
            self.dry_run = True

    def execute(
        self,
        ticker: str,
        action: str,
        assessment: RiskAssessment,
        current_price: float,
    ) -> ExecutionResult:
        """Execute an approved trade.

        Args:
            ticker: The ticker to trade.
            action: "BUY" or "SELL".
            assessment: The approved risk assessment with position sizing.
            current_price: Current market price.

        Returns:
            ExecutionResult with status and details.
        """
        result = ExecutionResult(
            ticker=ticker,
            action=action,
            shares=assessment.position_size_shares,
            price=current_price,
            timestamp=datetime.now().isoformat(),
        )

        # Final safety check
        if not assessment.approved:
            result.success = False
            result.message = "Trade not approved by risk manager."
            return result

        if assessment.position_size_shares <= 0:
            result.success = False
            result.message = "Position size is zero or negative."
            return result

        if self.dry_run:
            return self._execute_dry_run(result, assessment)
        else:
            return self._execute_live(result, assessment)

    def _execute_dry_run(
        self, result: ExecutionResult, assessment: RiskAssessment
    ) -> ExecutionResult:
        """Simulate trade execution without connecting to Alpaca."""
        result.success = True
        result.order_id = f"DRY-RUN-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        result.message = (
            f"[DRY RUN] Would {result.action} {result.shares:.4f} shares "
            f"of {result.ticker} at ~${result.price:.2f} "
            f"(${assessment.position_size_dollars:,.2f} total). "
            f"Stop loss: ${assessment.stop_loss_price:.2f}"
        )
        print(f"\n{'🔵' * 3} DRY RUN TRADE {'🔵' * 3}")
        print(result.message)
        return result

    def _execute_live(
        self, result: ExecutionResult, assessment: RiskAssessment
    ) -> ExecutionResult:
        """Submit a real order to Alpaca paper trading API."""
        try:
            from alpaca.trading.requests import MarketOrderRequest
            from alpaca.trading.enums import OrderSide, TimeInForce

            side = OrderSide.BUY if result.action == "BUY" else OrderSide.SELL

            order_request = MarketOrderRequest(
                symbol=result.ticker,
                qty=round(result.shares, 2),
                side=side,
                time_in_force=TimeInForce.DAY,
            )

            order = self._client.submit_order(order_request)
            result.success = True
            result.order_id = str(order.id)
            result.message = (
                f"Order submitted: {result.action} {result.shares:.4f} "
                f"shares of {result.ticker}. Order ID: {result.order_id}"
            )
        except Exception as e:
            result.success = False
            result.message = f"Alpaca order failed: {e}"

        return result

    def get_account_info(self) -> dict:
        """Get current Alpaca paper trading account info."""
        if self.dry_run or not self._client:
            return {
                "status": "dry_run",
                "equity": self.config.initial_capital,
                "cash": self.config.initial_capital,
                "buying_power": self.config.initial_capital * 2,
            }

        try:
            account = self._client.get_account()
            return {
                "status": account.status,
                "equity": float(account.equity),
                "cash": float(account.cash),
                "buying_power": float(account.buying_power),
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}
