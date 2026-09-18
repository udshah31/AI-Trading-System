"""
Browser Use Cloud - Smallest Working V4 Integration
====================================================
- V4 Hosted Agent (recommended) + Managed Browser helpers
- Reads BROWSER_USE_API_KEY server-side only (never logged/exposed)
- Defaults: model=gpt-5.6-luna, maxCostUsd=1.0 ($1 cap)
- Checks completed status, validates results, stops browsers / cancels abandoned runs
- Mock mode for tests (no spend)

Docs: https://docs.browser-use.com/cloud/llms.txt
      https://docs.browser-use.com/cloud/vibecoding
Pricing/credits: https://browser-use.com/pricing.md
Key creation: https://cloud.browser-use.com/settings?tab=api-keys&new=1
"""
from __future__ import annotations

import os
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional
from unittest.mock import MagicMock

DEFAULT_MODEL = "gpt-5.6-luna"
DEFAULT_MAX_COST_USD = 1.0  # $1 cap per run as requested


def get_api_key() -> str:
    """Return BROWSER_USE_API_KEY or raise with actionable message. Never logs the key."""
    key = os.getenv("BROWSER_USE_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "BROWSER_USE_API_KEY not set. Create one at "
            "https://cloud.browser-use.com/settings?tab=api-keys&new=1 "
            "(keys start with bu_) then export BROWSER_USE_API_KEY=bu_... "
            "Eligible social signups get $15 credit: https://browser-use.com/pricing.md"
        )
    if not key.startswith("bu_"):
        raise ValueError("BROWSER_USE_API_KEY should start with 'bu_'")
    return key


@dataclass
class AgentResult:
    run_id: str
    status: str
    result: Optional[str]
    is_success: bool
    cost_usd: Optional[str] = None
    live_url: Optional[str] = None


class BrowserUseCloudAgent:
    """
    Minimal V4 integration. Mock mode avoids any spend.
    Example:
        agent = BrowserUseCloudAgent(mock=True)  # tests
        agent = BrowserUseCloudAgent()           # live - requires BROWSER_USE_API_KEY
        result = agent.run_task("Find the top Hacker News story")
        assert result.is_success
    """

    def __init__(self, api_key: Optional[str] = None, mock: bool = False):
        self.mock = mock
        self._api_key = api_key  # if provided, use it; else get_api_key() lazily
        self._client = None
        if mock:
            m = MagicMock()
            # mock run lifecycle
            mock_run = MagicMock()
            mock_run.id = "mock-run-id"
            m.runs.create.return_value = mock_run
            mock_summary = MagicMock()
            mock_summary.status = MagicMock(value="completed")
            mock_summary.status.value = "completed"
            mock_summary.result = '{"title": "mock", "ok": true}'
            mock_summary.error = None
            m.runs.wait_for_completion.return_value = mock_summary
            # status returns completed
            mock_status = MagicMock()
            mock_status.status = MagicMock(value="completed")
            mock_status.status.value = "completed"
            m.runs.status.return_value = mock_status
            m.runs.get.return_value = mock_summary
            # browsers
            mock_browser = MagicMock()
            mock_browser.id = "mock-browser-id"
            mock_browser.cdp_url = "wss://mock.cdp"
            m.browsers.create.return_value = mock_browser
            self._client = m

    def _client_or_create(self):
        if self._client is not None:
            return self._client
        # live path - lazy key check so mock tests don't need env
        key = self._api_key or get_api_key()
        # Ask before live spending - caller should have confirmed
        from browser_use_sdk.v4 import BrowserUse

        self._client = BrowserUse(api_key=key)
        return self._client

    # ---------- V4 Hosted Agent ----------
    def run_task(
        self,
        task: str,
        model: str = DEFAULT_MODEL,
        max_cost_usd: float = DEFAULT_MAX_COST_USD,
        browser_settings: Optional[Dict[str, Any]] = None,
        proxy_country_code: Optional[str] = "us",
        session_id: Optional[str] = None,
        validate_json: bool = False,
    ) -> AgentResult:
        """
        Create a V4 run, wait for completion, validate, and handle cleanup.
        - model explicitly passed (required for TS typing; Python best practice)
        - max_cost_usd capped at $1 by default
        - if browser_settings present, proxyCountryCode must be set ("us" or None)
        - never retries ambiguous creates (caller must handle)
        """
        if not task or not task.strip():
            raise ValueError("task must be non-empty")
        if max_cost_usd > 1.0:
            # enforce cap unless explicitly overridden with confirmation
            raise ValueError("max_cost_usd capped at $1 by default; pass explicitly if you intend higher spend")

        client = self._client_or_create()

        # Build browserSettings with proxy trap handled
        bs = None
        if browser_settings is not None:
            # Ensure proxy_country_code is set when browserSettings present
            if "proxy_country_code" not in browser_settings and "proxyCountryCode" not in browser_settings:
                browser_settings = {**browser_settings, "proxy_country_code": proxy_country_code}
            bs = browser_settings

        created = None
        try:
            # Do not retry on ambiguous failure - let caller decide
            created = client.runs.create(
                task=task,
                model=model,
                max_cost_usd=max_cost_usd,
                browser_settings=bs,
                session_id=session_id,
            )
        except Exception as e:
            # Do not retry ambiguous creates per instructions
            raise RuntimeError(f"BrowserUse run create failed (not retried): {e}") from e

        run_id = str(getattr(created, "id", created))

        # Wait for completion - this polls status internally
        try:
            summary = client.runs.wait_for_completion(run_id)
        except Exception as e:
            # Abandoned run - attempt cancel to avoid orphaned spend
            try:
                client.runs.cancel(run_id)
            except Exception:
                pass
            raise RuntimeError(f"wait_for_completion failed for {run_id}: {e}") from e

        # Check completed status
        status_val = getattr(getattr(summary, "status", None), "value", str(getattr(summary, "status", "")))
        result_text = getattr(summary, "result", None)
        error_text = getattr(summary, "error", None)
        cost = getattr(summary, "total_cost_usd", None)

        # Status is enum Status2; terminal states: completed, failed, cancelled
        # We expose is_success based on result presence and not failed
        is_success = status_val == "completed" and not error_text

        # Validate results if requested
        if validate_json and result_text:
            try:
                json.loads(result_text)
            except json.JSONDecodeError as e:
                raise ValueError(f"Result is not valid JSON: {e}; raw: {result_text[:500]}") from e

        # Validate important external actions independently per docs
        # (caller should verify side effects; we just surface status)

        return AgentResult(
            run_id=run_id,
            status=status_val,
            result=result_text,
            is_success=is_success,
            cost_usd=str(cost) if cost is not None else None,
        )

    def get_status(self, run_id: str):
        """Cheap status poll (use vs full get)."""
        client = self._client_or_create()
        return client.runs.status(run_id)

    def cancel_run(self, run_id: str) -> None:
        """Cancel an abandoned run."""
        client = self._client_or_create()
        try:
            client.runs.cancel(run_id)
        except Exception as e:
            raise RuntimeError(f"cancel failed for {run_id}: {e}") from e

    # ---------- Managed Browser (CDP) ----------
    # Config: 15 min in United States (matches project default per your snippet)
    def create_browser(self, proxy_country_code: str = "us", timeout: int = 15, **kwargs):
        """
        Create a managed cloud browser (microVM, CDP).
        - Configuration: 15 min in United States (proxy_country_code="us", timeout=15)
        - Caller MUST stop via stop_browser(); billing runs until stop/timeout.
          client.close()/browser.close() does NOT stop billing.
        """
        client = self._client_or_create()
        return client.browsers.create(proxy_country_code=proxy_country_code, timeout=timeout, **kwargs)

    def stop_browser(self, browser_id: str) -> None:
        """
        Correctly stop a V4 browser and refund unused time.
        PATCH /api/v4/browsers/{id} {"action":"stop"} via SDK.
        """
        client = self._client_or_create()
        try:
            client.browsers.stop(browser_id)
        except Exception as e:
            raise RuntimeError(f"stop_browser failed for {browser_id}: {e}") from e
