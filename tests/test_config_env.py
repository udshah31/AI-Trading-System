""".env loading and LLM settings precedence: real env > root .env > TradingAgents/.env,
and explicit CLI settings > TRADINGAGENTS_* env > TradingAgents defaults."""
import os
import sys
import types

import pytest

from hybrid.config import HybridConfig, load_env_files


def test_root_env_wins_over_tradingagents_env_and_real_env_wins_over_both(tmp_path):
    names = ["HYB_TEST_ROOT_ONLY", "HYB_TEST_BOTH", "HYB_TEST_REAL"]
    (tmp_path / "TradingAgents").mkdir()
    (tmp_path / ".env").write_text("HYB_TEST_BOTH=root\nHYB_TEST_REAL=root\nHYB_TEST_ROOT_ONLY=root\n")
    (tmp_path / "TradingAgents" / ".env").write_text("HYB_TEST_BOTH=ta\nHYB_TEST_TA_ONLY=ta\n")
    os.environ["HYB_TEST_REAL"] = "real"
    try:
        load_env_files(tmp_path)
        assert os.environ["HYB_TEST_BOTH"] == "root"
        assert os.environ["HYB_TEST_ROOT_ONLY"] == "root"
        assert os.environ["HYB_TEST_TA_ONLY"] == "ta"
        assert os.environ["HYB_TEST_REAL"] == "real"
    finally:
        for name in names + ["HYB_TEST_TA_ONLY"]:
            os.environ.pop(name, None)


@pytest.fixture
def ta_defaults(monkeypatch):
    """Stand-in for tradingagents.default_config, whose DEFAULT_CONFIG already has
    TRADINGAGENTS_* env overrides applied (and type-coerced) at import time."""
    module = types.ModuleType("tradingagents.default_config")
    module.DEFAULT_CONFIG = {"llm_provider": "google", "deep_think_llm": "gemini-2.5-flash",
                             "quick_think_llm": "gemini-2.5-flash", "max_debate_rounds": 2}
    monkeypatch.setitem(sys.modules, "tradingagents", types.ModuleType("tradingagents"))
    monkeypatch.setitem(sys.modules, "tradingagents.default_config", module)
    return module.DEFAULT_CONFIG


def test_explicit_llm_settings_beat_env(ta_defaults, monkeypatch):
    monkeypatch.setenv("TRADINGAGENTS_LLM_PROVIDER", "google")
    cfg = HybridConfig(tradingagents_config={"llm_provider": "anthropic", "deep_think_llm": "m"})
    assert cfg.tradingagents_config["llm_provider"] == "anthropic"
    assert cfg.tradingagents_config["deep_think_llm"] == "m"
    assert cfg.tradingagents_config["quick_think_llm"] == "gemini-2.5-flash"


def test_env_config_is_not_reapplied_as_strings(ta_defaults, monkeypatch):
    monkeypatch.setenv("TRADINGAGENTS_MAX_DEBATE_ROUNDS", "2")
    assert HybridConfig().tradingagents_config["max_debate_rounds"] == 2  # int, not "2"


def _run_module():
    import importlib.util
    spec = importlib.util.spec_from_file_location("run_cli", "run.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_cli_passes_only_llm_flags_that_were_given():
    run = _run_module()
    parser = run.build_parser()
    assert run.tradingagents_overrides(parser.parse_args(["-t", "AAPL"]), parser) == {}
    args = parser.parse_args(["-t", "AAPL", "--llm-provider", "anthropic", "--llm-model", "m"])
    assert run.tradingagents_overrides(args, parser) == {
        "llm_provider": "anthropic", "deep_think_llm": "m", "quick_think_llm": "m"}


def test_cli_rejects_provider_without_model():
    run = _run_module()
    parser = run.build_parser()
    args = parser.parse_args(["-t", "AAPL", "--llm-provider", "anthropic"])
    with pytest.raises(SystemExit):
        run.tradingagents_overrides(args, parser)
