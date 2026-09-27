# LLM analysis in the live loop — design

Date: 2026-09-27
Status: approved in conversation, awaiting spec review

## Goal

The live bot (`hybrid/main_async.py`) decides every 5 minutes from technical indicators only:
`QuantAgent` runs `HybridPipeline(skip_llm=True)`, so every cycle logs
`LLM Signals: [SKIPPED] Using default neutral signals`. The TradingAgents LLM analysis already
exists as `LLMAnalystAgent` (`hybrid/llm_agent.py`) but nothing ever asks it to run.

Make live decisions use the LLM analysis, with the existing weights (LLM 0.75, technicals 0.25,
BUY ≥ 0.65, SELL ≤ 0.35).

## Decisions (from the user)

- The LLM analysis runs **every 4 hours** per coin, not every 5-minute cycle.
- **Existing weights stay.** No weight or threshold changes.

## Current behaviour worth keeping

`compute_decision` (`hybrid/quant_engine.py`) already handles missing LLM data: when
`llm_signals.available` is false it drops the LLM track and scores technicals alone on the full
0–1 scale. The live bot uses this today, so "no LLM data" means "trade on technicals", not
"never trade".

## Design

Chosen approach: `QuantAgent` keeps the latest LLM signals per ticker and passes them into each
decision. Rejected: routing through `SignalFusionAgent` (it fuses one LLM result with one quant
result and then clears both, so it cannot reuse a 4-hour-old analysis every 5 minutes, and it
would add a second decision message type), and running the full pipeline on a slow timer (mixes
the slow loop into the fast one and leaves no LLM input between runs).

### Data flow

1. **Scheduler** (`main_async.py`): at startup, then every `LLM_INTERVAL_HOURS`, publish one
   `llm_analysis_request` per traded pair (`BTC/USDT`, `ETH/USDT`) to `llm_analyst`, with
   `asset_type="crypto"`, today's date, and the Yahoo symbol from `market_data_symbol`
   (`BTC-USD`). The request carries the exchange pair too, so results map back to the pair the
   bot trades.
2. **LLMAnalystAgent** runs TradingAgents (unchanged logic), records the analysis for the
   dashboard Signals panel (unchanged), and publishes `llm_analysis_result`.
3. **QuantAgent** subscribes to `llm_analysis_result`. On success it stores
   `{signals, received_at}` for the exchange pair; failures are ignored (last good signals stay).
4. **Each 5-minute cycle**, `QuantAgent` looks up the stored signals. If present and at most
   `LLM_MAX_AGE_HOURS` (8) old, it passes them to the pipeline; otherwise it passes nothing and
   the decision is technicals-only, exactly as today.
5. `quant_decision` gains `llm_component` and `llm_age_hours` (null when technicals-only).
   The orchestrator's log line shows the source: `(LLM 1.2h old)` or `(technicals only)`.

Risk agent, orchestrator order routing and execution do not change.

### Pipeline change

`HybridPipeline.analyze_quant_only` gains an optional `llm_signals` argument. When given, Step 1
uses those signals instead of skipping (no TradingAgents call); when absent, behaviour is
unchanged.

### Bug fix required by the wiring

`LLMAnalystAgent._publish_result` omits `available`, `sources` and `confidences`. A receiver that
rebuilds `LLMSignals(**data)` gets `available=False` and the signals would be ignored. The
published dict must include all three.

### Errors, timeouts, concurrency

- Failed or timed-out analysis: logged (`[LLM] BTC/USDT analysis failed: …`), never overwrites
  the stored signals.
- Timeout raised from 300 s to 900 s (a full debate on the 1-OCPU VM can exceed 5 minutes).
  `asyncio.wait_for` stops waiting; the worker thread may still finish in the background.
- `max_concurrent` for the live analyst becomes 1: the agent shares one `TradingAgentsGraph`
  instance, which is not safe to run concurrently. BTC then ETH every 4 hours is enough.

### Configuration (`.env`, read at startup)

| Variable | Default | Meaning |
|---|---|---|
| `LLM_ENABLED` | `true` | `false` sends no requests; bot is technicals-only |
| `LLM_INTERVAL_HOURS` | `4` | time between analysis rounds |
| `LLM_MAX_AGE_HOURS` | `8` | older stored signals are ignored |

### Cost

2 pairs × 6 rounds/day = 12 TradingAgents runs/day (one debate round each) on
`gemini-2.5-flash`, plus one extra round at every restart (each auto-deploy). Signals live in
memory; after a restart decisions are technicals-only until the startup round completes.

## Testing (TDD, fake bus, no real LLM calls)

1. A stored fresh result is used by the next decision: `llm_component > 0` and the action
   follows the LLM-weighted score.
2. No stored result, or one older than `LLM_MAX_AGE_HOURS`: decision is technicals-only
   (same as today).
3. A failed result does not replace earlier good signals.
4. `llm_analysis_result` round-trips `available=True` (and `sources`, `confidences`).
5. Scheduler publishes requests for both pairs at startup and after each interval (injected
   clock/sleep), and none when `LLM_ENABLED=false`.

Existing suites stay green; `mypy hybrid/ --ignore-missing-imports` clean.

## Verification after deploy

On the VM: `docker compose logs trading-system` shows `LLM analysis complete for BTC/USDT` and
decisions tagged `(LLM …h old)`; the dashboard Signals panel lists the new analyses.

## Out of scope

- Persisting LLM signals across restarts.
- Changing weights, thresholds or `SignalFusionAgent`.
- Trading pairs other than BTC/USDT and ETH/USDT.
