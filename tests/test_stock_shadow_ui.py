"""Execute the real dashboard renderers; no browser or network required."""
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest


pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="Node.js required for dashboard renderer checks")


def run_ui_checks(tmp_path, checks, fixtures=""):
    page = Path("hybrid/dashboard/index.html").read_text()
    script = "\n".join(re.findall(r"<script[^>]*>(.*?)</script>", page, re.S))
    harness = r'''
const assert = require('node:assert/strict');
const elements = { stockShadowRows: { innerHTML: '', querySelectorAll() { return []; }, contains() { return false; } },
  decisionRows: { innerHTML: '' } };
global.document = {
  addEventListener() {},
  querySelectorAll() { return []; },
  getElementById(id) { return elements[id] ||= { textContent: '', innerHTML: '', className: '' }; }
};
global.window = {};
global.history = { replaceState() {} };
global.fetch = async () => { throw new Error('offline'); };
'''
    path = tmp_path / "dashboard-ui.cjs"
    path.write_text(harness + "\n" + fixtures + "\n" + script + "\n" + checks)
    subprocess.run(["node", "--check", str(path)], check=True, capture_output=True, text=True)
    result = subprocess.run(["node", str(path)], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr


def stock_fixtures():
    from hybrid.stock_shadow import RANKING, SYMBOLS, WATCHLIST
    return (f"const WATCHLIST = {json.dumps(WATCHLIST)}; const SYMBOLS = {json.dumps(SYMBOLS)}; "
            f"const RANKING = {json.dumps(RANKING)};")


def test_stock_table_coverage_signals_and_filtering(tmp_path):
    run_ui_checks(tmp_path, r'''
const base = { enabled: false, decisions: [], buy_threshold: .7, sell_threshold: .3, watchlist: WATCHLIST, symbols: SYMBOLS, ranking: RANKING };
renderStockShadow(base);
assert.match(elements.stockShadowNote.textContent, /Research is off/);
assert.match(elements.stockShadowRows.innerHTML, /S&amp;P 500 ETF/);
assert.match(elements.stockShadowRows.innerHTML, /Nasdaq-100/);
assert.equal((elements.stockShadowRows.innerHTML.match(/<tr data-stock=/g) || []).length, 102);
assert.match(elements.stockRankingNote.textContent, /Ranking snapshot retrieved/);
assert.match(elements.stockShadowRows.innerHTML, /Sector rank #1/);
assert.match(elements.stockShadowRows.innerHTML, /Ranking source/);
for (const asset of WATCHLIST) {
  assert.match(elements.stockShadowRows.innerHTML, new RegExp(`data-stock="${asset.symbol}"`));
}
assert.match(elements.stockShadowRows.innerHTML, /IT \/ Technology/);
assert.match(elements.stockShadowRows.innerHTML, /Healthcare/);
assert.match(elements.stockShadowRows.innerHTML, /Waiting for data/);
assert.match(elements.stockShadowRows.innerHTML, /0.70/);
renderStockShadow({ ...base, enabled: true });
assert.match(elements.stockShadowNote.textContent, /Waiting for the first result/);
const decision = {symbol: 'SPY', action: 'buy', score: .8, session_date: '<img src=x onerror=alert(1)>',
  close_price: 500, analyzed_at: '2026-11-27T18:20:00Z'};
const decisions = [decision, {...decision, symbol: 'QQQ', action: 'sell', score: .2},
  {...decision, symbol: 'AAPL', action: 'hold', score: .99}];
renderStockShadow({ ...base, enabled: true, decisions });
const row = symbol => elements.stockShadowRows.innerHTML.match(new RegExp(`<tr data-stock="${symbol}"[^>]*>[\\s\\S]*?</tr>`))[0];
assert.match(row('SPY'), /Buy signal/);
assert.match(row('QQQ'), /Sell signal/);
assert.match(row('AAPL'), />Wait</);
assert.doesNotMatch(row('AAPL'), /Buy signal/);
assert.match(row('SPY'), /\$500\.00/);
assert.match(elements.stockShadowRows.innerHTML, /0.800/);
assert.match(elements.stockShadowRows.innerHTML, /&lt;img/);
assert.doesNotMatch(elements.stockShadowRows.innerHTML, /<img/);
assert.match(elements.stockShadowNote.textContent, /not live prices/);
assert.doesNotMatch(elements.stockShadowRows.innerHTML, /Place order|Buy now|Sell now/);
renderStockShadow({ ...base, decisions: [decision] });
assert.match(elements.stockShadowNote.textContent, /historical/);
elements.stockSectorFilter.value = 'Healthcare';
renderStockShadow(base);
assert.equal((elements.stockShadowRows.innerHTML.match(/<tr data-stock=/g) || []).length, 10);
assert.match(elements.stockShadowRows.innerHTML, /Healthcare/);
assert.doesNotMatch(elements.stockShadowRows.innerHTML, /data-stock="NVDA"/);
assert.match(elements.stockCoverage.textContent, /0 of 10/);
renderStockShadow({ ...base, enabled: true });
assert.equal(elements.stockSectorFilter.value, 'Healthcare');
elements.stockSectorFilter.value = 'ETF benchmarks';
renderStockShadow(base);
assert.equal((elements.stockShadowRows.innerHTML.match(/<tr data-stock=/g) || []).length, 2);
assert.doesNotMatch(elements.stockShadowRows.innerHTML, /Sector rank/);
elements.stockSectorFilter.value = 'unknown-sector';
renderStockShadow(base);
assert.equal(elements.stockSectorFilter.value, '');
assert.equal((elements.stockShadowRows.innerHTML.match(/<tr data-stock=/g) || []).length, 102);
renderStockShadow({...base, enabled: true, decisions: [{...decision, action: 'unexpected'}]});
assert.match(row('SPY'), /Unknown signal/);
assert.doesNotMatch(row('SPY'), /Buy signal|Sell signal|>Wait</);
''', stock_fixtures())


def test_crypto_table_uses_reported_actions_not_current_thresholds(tmp_path):
    run_ui_checks(tmp_path, r'''
const coins = [{symbol: 'BTC/USDT', action: 'BUY', score: .8},
  {symbol: 'ETH/USDT', action: 'hold', score: .99},
  {symbol: 'SOL/USDT', action: 'sell', score: .1},
  {symbol: '<img src=x>/USDT', action: 'unexpected', score: .5}];
renderDecisionTable(coins);
const row = symbol => elements.decisionRows.innerHTML.match(new RegExp(`<tr data-coin="${symbol}">[\\s\\S]*?</tr>`))[0];
assert.match(row('BTC/USDT'), /Buy signal/);
assert.match(row('ETH/USDT'), />Wait</);
assert.doesNotMatch(row('ETH/USDT'), /Buy signal/);
assert.match(row('SOL/USDT'), /Sell signal/);
assert.match(elements.decisionRows.innerHTML, /Unknown signal/);
assert.match(elements.decisionRows.innerHTML, /&lt;img/);
assert.doesNotMatch(elements.decisionRows.innerHTML, /<img|<button/);
assert.match(verdict(coins), /1 buy signal/);
assert.match(verdict(coins), /1 sell signal/);
assert.match(verdict(coins), /1 wait/);
renderDecisionTable([]);
assert.match(elements.decisionRows.innerHTML, /Waiting for data/);
assert.match(verdict([]), /Waiting/);
''')


def test_refresh_failure_survives_filtering_and_mode_becomes_unknown(tmp_path):
    run_ui_checks(tmp_path, r'''
const base = { enabled: true, decisions: [], buy_threshold: .7, sell_threshold: .3, watchlist: WATCHLIST, symbols: SYMBOLS, ranking: RANKING };
renderStockShadow(base);
(async () => {
  await assert.rejects(loadStockShadow(), /offline/);
  assert.match(elements.stockShadowNote.textContent, /previously loaded snapshots/);
  assert.match(elements.stockShadowNote.className, /error/);
  selectView('research');
  assert.match(elements.stockShadowNote.textContent, /previously loaded snapshots/);
  assert.match(elements.stockShadowNote.className, /error/);
  renderStockShadow(base);
  assert.match(elements.stockShadowNote.className, /error/);
  global.fetch = async () => ({ ok: true, status: 200, json: async () => base });
  await loadStockShadow();
  assert.doesNotMatch(elements.stockShadowNote.className, /error/);
  global.fetch = async () => { throw new Error('offline'); };
  renderMode('live');
  await assert.rejects(loadBand(), /offline/);
  assert.match(elements.modeTitle.textContent, /unknown/i);
  assert.doesNotMatch(elements.modeCard.className, /safe|live/);
  assert.match(elements.modeExplanation.textContent, /could not be verified/i);
  assert.match(elements.stamp.textContent, /unknown/i);
})().catch(error => { console.error(error); process.exitCode = 1; });
''', stock_fixtures())


def test_missing_or_nonfinite_scores_remove_previous_chart_pins(tmp_path):
    run_ui_checks(tmp_path, r'''
for (const score of [null, NaN, Infinity]) {
  const pins = [{dataset: {symbol: 'BTC/USDT'}, remove() { pins.length = 0; }}];
  elements.track = {clientWidth: 800, querySelectorAll() { return [...pins]; },
    closest() { return {classList: {toggle() {}}}; }};
  for (const id of ['zoneSell', 'zoneBuy', 'lineSell', 'lineBuy', 'scale']) elements[id] = {style: {}};
  elements.bandDetails = {open: true};
  band.coins = new Map([['BTC/USDT', {symbol: 'BTC/USDT', action: 'hold', score}]]);
  renderBand();
  assert.match(elements.decisionRows.innerHTML, />Wait</);
  assert.equal(pins.length, 0, 'A previous valid-score pin must disappear when its score is unavailable');
  assert.equal(elements.bandList.innerHTML, '');
}
''')


def test_control_room_shell_and_risk_errors_are_explicit(tmp_path):
    page = Path("hybrid/dashboard/index.html").read_text()
    for marker in (
        'class="mode-card trust-strip"',
        'class="kpi-grid"',
        'class="band decision-board"',
        'id="riskStatus"',
        'id="overviewAttention"',
    ):
        assert marker in page
    assert "Trading control room" in page

    run_ui_checks(tmp_path, r'''
renderLimits({error: 'risk service offline'});
assert.match(elements.riskStatus.textContent, /unavailable/i);
assert.match(elements.limits.innerHTML, /could not be verified/i);
assert.doesNotMatch(elements.limits.innerHTML, /0\.0% of 15\.0%/);
''')


def test_decisions_expose_score_and_stock_research_can_be_searched(tmp_path):
    run_ui_checks(tmp_path, r'''
renderDecisionTable([{symbol: 'BTC/USDT', action: 'buy', score: .78}]);
assert.match(elements.decisionRows.innerHTML, /Score <strong>0\.78/);
assert.match(elements.decisionRows.innerHTML, /View evidence/);

elements.stockSearch = {value: 'Apple'};
const base = {enabled: false, decisions: [], buy_threshold: .7, sell_threshold: .3,
  watchlist: WATCHLIST, symbols: SYMBOLS, ranking: RANKING};
renderStockShadow(base);
assert.equal((elements.stockShadowRows.innerHTML.match(/<tr data-stock=/g) || []).length, 1);
assert.match(elements.stockShadowRows.innerHTML, /AAPL/);
''', stock_fixtures())


def test_stock_table_groups_every_category_and_shows_research_context(tmp_path):
    run_ui_checks(tmp_path, r'''
const decision = {symbol: 'AAPL', action: 'buy', score: .8, confidence: .72,
  session_date: '2026-11-27', close_price: 500, analyzed_at: '2026-11-27T18:20:00Z'};
renderStockShadow({enabled: true, decisions: [decision], buy_threshold: .7, sell_threshold: .3,
  watchlist: WATCHLIST, symbols: SYMBOLS, ranking: RANKING});
assert.equal((elements.stockShadowRows.innerHTML.match(/class="category-row"/g) || []).length, 11);
assert.match(elements.stockShadowRows.innerHTML, /ETF benchmarks/);
assert.match(elements.stockShadowRows.innerHTML, /IT \/ Technology/);
assert.match(elements.stockShadowRows.innerHTML, /1\/10 analyzed/);
assert.match(elements.stockShadowRows.innerHTML, /Buy 1/);
assert.match(elements.stockShadowRows.innerHTML, /Confidence/);
assert.match(elements.stockShadowRows.innerHTML, /Market cap/);
const aapl = elements.stockShadowRows.innerHTML.match(/<tr data-stock="AAPL"[^>]*>[\s\S]*?<\/tr>/)[0];
assert.match(aapl, /72%/);
assert.match(elements.stockShadowRows.innerHTML, /Communication services/);
assert.equal((elements.stockShadowRows.innerHTML.match(/<tr data-stock=/g) || []).length, 102);
''', stock_fixtures())


def test_category_counts_and_missing_evidence_are_not_invented(tmp_path):
    run_ui_checks(tmp_path, r'''
const assets = [{symbol: 'A', name: 'Alpha', sector: 'Example', kind: 'stock'},
  {symbol: 'B', name: 'Beta', sector: 'Example', kind: 'stock'},
  {symbol: 'C', name: 'Gamma', sector: 'Example', kind: 'stock'},
  {symbol: 'D', name: 'Delta', sector: 'Example', kind: 'stock'}];
const state = {enabled: true, watchlist: assets, buy_threshold: .7, sell_threshold: .3,
  decisions: [{symbol: 'A', action: 'buy', score: .8, confidence: 0, features: {
      rsi: .25, ema: .9, bollinger: .5, volume: .75, weights: {rsi: .3},
      buy_threshold: .65, sell_threshold: .35, source: 'yahoo_daily_adjusted'}},
    {symbol: 'B', action: 'hold', score: null, confidence: null},
    {symbol: 'C', action: 'invalid', score: Infinity, confidence: -1}]};
renderStockShadow(state);
assert.match(elements.stockShadowRows.innerHTML, /3\/4 analyzed/);
assert.match(elements.stockShadowRows.innerHTML, /Buy 1/);
assert.match(elements.stockShadowRows.innerHTML, /Wait 1/);
assert.match(elements.stockShadowRows.innerHTML, /Unknown 1/);
assert.match(elements.stockShadowRows.innerHTML, /Waiting 1/);
assert.match(elements.stockShadowRows.innerHTML, /0\.800/); // Only the finite, non-null score contributes.
assert.match(elements.stockShadowRows.innerHTML, /0\.250/);
assert.match(elements.stockShadowRows.innerHTML, /Saved thresholds: buy ≥ 0\.65/);
assert.match(elements.stockShadowRows.innerHTML, /Confidence<\/dt><dd>0%/);
assert.match(elements.stockShadowRows.innerHTML, /Confidence<\/dt><dd>Unavailable/);
assert.doesNotMatch(elements.stockShadowRows.innerHTML, /NaN|Infinity|-100%/);
setStockCategoriesCollapsed(true);
assert.equal((elements.stockShadowRows.innerHTML.match(/role="row" hidden/g) || []).length, 4);
renderStockShadow(state);
assert.equal((elements.stockShadowRows.innerHTML.match(/role="row" hidden/g) || []).length, 4);
setStockCategoriesCollapsed(false);
elements.stockSignalFilter.value = 'pending';
renderStockShadow(state);
assert.equal((elements.stockShadowRows.innerHTML.match(/<tr data-stock=/g) || []).length, 1);
assert.match(elements.stockShadowRows.innerHTML, /data-stock="D"/);
elements.stockSignalFilter.value = '';
elements.stockSearch.value = 'unmatched';
renderStockShadow(state);
assert.match(elements.stockShadowRows.innerHTML, /No matches/);
''')


def test_category_summary_and_stock_rows_share_one_research_table(tmp_path):
    page = Path("hybrid/dashboard/index.html").read_text()
    research = page.split('id="researchPanel"', 1)[1].split('id="advancedPanel"', 1)[0]
    assert 'id="stockCategories"' not in research
    assert research.count('<table') == 1
    run_ui_checks(tmp_path, r'''
const base = {enabled: false, decisions: [], buy_threshold: .7, sell_threshold: .3,
  watchlist: WATCHLIST, symbols: SYMBOLS, ranking: RANKING};
renderStockShadow(base);
assert.equal((elements.stockShadowRows.innerHTML.match(/class="category-row"/g) || []).length, 11);
assert.match(elements.stockShadowRows.innerHTML, /<tr class="category-row"[\s\S]*?IT \/ Technology/);
assert.match(elements.stockShadowRows.innerHTML, /<tr class="category-row"[\s\S]*?ETF benchmarks/);
''', stock_fixtures())


def test_refresh_keeps_focus_when_signal_filter_removes_focused_category(tmp_path):
    run_ui_checks(tmp_path, r'''
const state = {enabled: true, watchlist: [{symbol: 'A', name: 'Alpha', sector: 'Example', kind: 'stock'}],
  decisions: [{symbol: 'A', action: 'buy', score: .8}], buy_threshold: .7, sell_threshold: .3};
renderStockShadow(state);
elements.stockSignalFilter.value = 'buy';
document.activeElement = {dataset: {category: 'Example'}};
global.CSS = {escape: value => value};
elements.stockSignalFilter.focus = function () { document.activeElement = this; };
elements.stockShadowRows.querySelector = () => null;
Object.defineProperty(elements.stockShadowRows, 'innerHTML', {
  set(value) { this.markup = value; document.activeElement = null; },
  get() { return this.markup; }
});
renderStockShadow({...state, decisions: [{symbol: 'A', action: 'sell', score: .2}]});
assert.match(elements.stockShadowRows.innerHTML, /No matches/);
assert.equal(document.activeElement, elements.stockSignalFilter);
''')
