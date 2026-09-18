import json
import os
import pytest
from unittest.mock import MagicMock, patch

from hybrid.browser_use_cloud import BrowserUseCloudAgent, DEFAULT_MODEL, DEFAULT_MAX_COST_USD, get_api_key


def test_get_api_key_missing(monkeypatch):
    monkeypatch.delenv("BROWSER_USE_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="BROWSER_USE_API_KEY not set"):
        get_api_key()


def test_get_api_key_bad_prefix(monkeypatch):
    monkeypatch.setenv("BROWSER_USE_API_KEY", "sk_bad")
    with pytest.raises(ValueError, match="start with 'bu_'"):
        get_api_key()


def test_mock_run_task_success():
    agent = BrowserUseCloudAgent(mock=True)
    res = agent.run_task("Find the top Hacker News story")
    assert res.is_success
    assert res.status == "completed"
    assert res.run_id == "mock-run-id"
    # default model and cost cap enforced
    agent._client.runs.create.assert_called_once()
    call_kwargs = agent._client.runs.create.call_args.kwargs
    assert call_kwargs["model"] == DEFAULT_MODEL
    assert call_kwargs["max_cost_usd"] == DEFAULT_MAX_COST_USD


def test_mock_validate_json():
    agent = BrowserUseCloudAgent(mock=True)
    # mock returns valid JSON by default
    res = agent.run_task("Return JSON", validate_json=True)
    json.loads(res.result)


def test_mock_validate_json_invalid():
    agent = BrowserUseCloudAgent(mock=True)
    # make mock return invalid JSON
    agent._client.runs.wait_for_completion.return_value.result = "not json"
    with pytest.raises(ValueError, match="not valid JSON"):
        agent.run_task("Return JSON", validate_json=True)


def test_max_cost_cap_enforced():
    agent = BrowserUseCloudAgent(mock=True)
    with pytest.raises(ValueError, match="capped at \\$1"):
        agent.run_task("task", max_cost_usd=5.0)


def test_empty_task_rejected():
    agent = BrowserUseCloudAgent(mock=True)
    with pytest.raises(ValueError):
        agent.run_task("   ")


def test_no_retry_on_create_failure():
    agent = BrowserUseCloudAgent(mock=True)
    agent._client.runs.create.side_effect = RuntimeError("ambiguous 500")
    with pytest.raises(RuntimeError, match="not retried"):
        agent.run_task("task")
    assert agent._client.runs.create.call_count == 1  # no retry


def test_cancel_abandoned_on_wait_failure():
    agent = BrowserUseCloudAgent(mock=True)
    agent._client.runs.wait_for_completion.side_effect = TimeoutError("timeout")
    with pytest.raises(RuntimeError, match="wait_for_completion failed"):
        agent.run_task("task")
    agent._client.runs.cancel.assert_called_once()


def test_browser_create_stop_mock():
    agent = BrowserUseCloudAgent(mock=True)
    b = agent.create_browser(proxy_country_code="us")
    assert b.id == "mock-browser-id"
    agent.stop_browser(b.id)
    agent._client.browsers.stop.assert_called_once_with(b.id)


def test_live_requires_key(monkeypatch):
    monkeypatch.delenv("BROWSER_USE_API_KEY", raising=False)
    agent = BrowserUseCloudAgent(mock=False)
    with pytest.raises(RuntimeError, match="BROWSER_USE_API_KEY not set"):
        agent.run_task("task")
