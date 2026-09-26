"""
Record LLM analyses for the dashboard's Signals panel
======================================================
Called right after a TradingAgents run is scored, from synchronous code (the ``run.py``
pipeline, or ``llm_agent``'s worker thread). Needs DATABASE_URL; never raises, because
failing to record must not fail the analysis.
"""
import asyncio
import os
from typing import Optional

from hybrid.signal_extractor import LLMSignals


def record_llm_analysis(final_state: dict, signals: LLMSignals, ticker: str, trade_date: str,
                        database_url: Optional[str] = None) -> Optional[str]:
    """Store the run; returns its id, or None when not configured or on any error."""
    url = database_url or os.getenv("DATABASE_URL")
    if not url:
        return None
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass  # no loop in this thread: safe to run our own below
    else:
        print("[LLMRecords] Skipped: called from inside a running event loop")
        return None
    try:
        return asyncio.run(_save(url, final_state, signals, ticker, trade_date))
    except Exception as e:
        print(f"[LLMRecords] Could not record analysis for {ticker}: {e}")
        return None


async def _save(url: str, final_state: dict, signals: LLMSignals, ticker: str, trade_date: str) -> str:
    from hybrid.storage import StorageService
    storage = StorageService(url)
    try:
        await storage.initialize()  # creates the tables on first use
        return await storage.llm.save_analysis(ticker, trade_date, final_state, signals)
    finally:
        await storage.close()
