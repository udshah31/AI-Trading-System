"""Behaviour tests for the 2026-09-18 P0 fixes (ML weights, SQLite storage, Jupiter URL,
Uniswap deadline). The funding-rate fix is covered by test_btc_funding_units.py."""
import asyncio
import json
import time
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import hybrid.config as config_module
from hybrid.config import HybridConfig


def test_learned_weights_scale_into_tech_budget(tmp_path, monkeypatch):
    weights = tmp_path / "learned_weights.json"
    weights.write_text(json.dumps({"tech_weight_rsi": 0.4, "tech_weight_ema": 0.3,
                                   "tech_weight_bollinger": 0.2, "tech_weight_volume": 0.1}))
    monkeypatch.setattr(config_module, "LEARNED_WEIGHTS_PATH", weights)

    cfg = HybridConfig()
    assert cfg.weight_rsi == pytest.approx(0.10)
    assert cfg.weight_ema_crossover == pytest.approx(0.075)
    assert cfg.weight_bollinger == pytest.approx(0.05)
    assert cfg.weight_volume == pytest.approx(0.025)


def test_defaults_without_learned_weights(tmp_path, monkeypatch):
    monkeypatch.setattr(config_module, "LEARNED_WEIGHTS_PATH", tmp_path / "missing.json")
    cfg = HybridConfig()
    assert (cfg.weight_rsi, cfg.weight_ema_crossover) == (0.10, 0.08)


def test_storage_round_trips_on_sqlite(tmp_path):
    pytest.importorskip("aiosqlite")
    from hybrid.storage import StorageService

    async def scenario():
        svc = StorageService(f"sqlite+aiosqlite:///{tmp_path / 'test.db'}")
        await svc.initialize()
        try:
            await svc.record_equity(timestamp=datetime.now(timezone.utc), account="default",
                                    equity=Decimal("100000.00"), drawdown_pct=Decimal("0.01"))
            return await svc.get_equity_history("default", days=1)
        finally:
            await svc.close()

    [point] = asyncio.run(scenario())
    assert point.equity == Decimal("100000.00")


def test_jupiter_quotes_from_lite_api():
    from solders.keypair import Keypair
    from hybrid.dex_executor import JupiterExecutor, QuoteRequest

    requested = []

    class Response:
        status = 200

        async def json(self):
            return {"inAmount": "1000", "outAmount": "990", "priceImpactPct": "0.1", "routePlan": []}

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            pass

    executor = JupiterExecutor(private_key=str(Keypair()))
    executor.session = SimpleNamespace(get=lambda url, params: requested.append(url) or Response())
    quote = asyncio.run(executor.get_quote(QuoteRequest(
        input_mint="in", output_mint="out", amount=1000, slippage_bps=50)))

    assert requested[0].startswith("https://lite-api.jup.ag/")
    assert (quote.input_amount, quote.output_amount) == (1000, 990)


def test_uniswap_deadline_is_unix_epoch():
    from hybrid.dex_executor import QuoteResponse, UniswapV3Executor

    executor = UniswapV3Executor.__new__(UniswapV3Executor)
    executor.wrapped_native = "WETH"
    executor.address = "0xabc"
    executor.config = {"router": "0xrouter", "chain_id": 8453}
    executor.gas_multiplier = 1.0
    executor.router = MagicMock()
    executor.account = MagicMock()
    executor.w3 = MagicMock()
    executor.w3.eth.gas_price = 1
    executor.w3.eth.get_transaction_count.return_value = 0
    # web3 returns receipts as AttributeDict (TxReceipt): key and attribute access
    from web3.datastructures import AttributeDict
    executor.w3.eth.wait_for_transaction_receipt.return_value = AttributeDict(
        {"status": 1, "gasUsed": 1, "effectiveGasPrice": 1})

    quote = QuoteResponse(input_amount=1000, output_amount=990, price_impact_pct=0.1,
                          route=[{"token_in": "WETH", "token_out": "TOKEN", "fee": 3000}],
                          dex=None, valid_for_seconds=20)
    asyncio.run(executor.execute_swap(quote))

    params = executor.router.functions.exactInputSingle.call_args.args[0]
    assert time.time() + 290 < params["deadline"] <= time.time() + 300


def test_evm_executors_available_on_installed_web3():
    # web3 >= 7 renamed geth_poa_middleware; the failed import silently disabled every EVM chain
    from hybrid.dex_executor import EVM_AVAILABLE
    assert EVM_AVAILABLE is True


def test_jupiter_swap_sends_a_validly_signed_transaction():
    import base64
    from solders.hash import Hash
    from solders.keypair import Keypair
    from solders.message import MessageV0
    from solders.system_program import TransferParams, transfer
    from solders.transaction import VersionedTransaction
    from hybrid.dex_executor import JupiterExecutor, QuoteResponse

    owner = Keypair()
    executor = JupiterExecutor(private_key=str(owner))
    message = MessageV0.try_compile(owner.pubkey(), [transfer(TransferParams(
        from_pubkey=owner.pubkey(), to_pubkey=Keypair().pubkey(), lamports=1))], [], Hash.default())
    unsigned = VersionedTransaction.populate(message, [])  # what Jupiter's swap API returns
    sent = []

    class Response:
        status = 200

        def __init__(self, body):
            self.body = body

        async def json(self):
            return self.body

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            pass

    def post(url, json):
        if "swap" in url:
            return Response({"swapTransaction": base64.b64encode(bytes(unsigned)).decode()})
        if json["method"] == "sendTransaction":
            sent.append(VersionedTransaction.from_bytes(base64.b64decode(json["params"][0])))
            return Response({"result": "sig"})
        return Response({"result": {"value": [{"confirmationStatus": "confirmed", "err": None}]}})

    executor.session = SimpleNamespace(post=post)
    quote = QuoteResponse(input_amount=1, output_amount=1, price_impact_pct=0.0, route=[], dex=None)
    result = asyncio.run(executor.execute_swap(quote))

    assert result.success, result.error
    [tx] = sent
    assert tx.verify_with_results() == [True]  # signed by the owner's key
