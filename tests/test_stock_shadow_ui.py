"""Execute the real dashboard renderer with a minimal DOM; no browser/network required."""
import re
import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js required for dashboard renderer checks")
def test_stock_cards_and_beginner_states(tmp_path):
    page = Path("hybrid/dashboard/index.html").read_text()
    import json
    from hybrid.stock_shadow import RANKING, SYMBOLS, WATCHLIST
    script = "\n".join(re.findall(r"<script[^>]*>(.*?)</script>", page, re.S))
    harness = r'''
const assert = require('node:assert/strict');
const elements = {};
global.document = {
  addEventListener() {},
  getElementById(id) { return elements[id] ||= { textContent: '', innerHTML: '', className: '' }; }
};
global.window = {};
global.fetch = async () => { throw new Error('offline'); };
'''
    checks = r'''
const base = { enabled: false, decisions: [], buy_threshold: .7, sell_threshold: .3, watchlist: WATCHLIST, symbols: SYMBOLS, ranking: RANKING };
renderStockShadow(base);
assert.match(elements.stockShadowNote.textContent, /Research is off/);
assert.match(elements.stockShadowCards.innerHTML, /S&amp;P 500 ETF/);
assert.match(elements.stockShadowCards.innerHTML, /Nasdaq-100/);
assert.equal((elements.stockShadowCards.innerHTML.match(/<article/g) || []).length, 102);
assert.equal((elements.stockShadowCards.innerHTML.match(/<details/g) || []).length, 11);
assert.match(elements.stockRankingNote.textContent, /Ranking snapshot retrieved/);
assert.match(elements.stockShadowCards.innerHTML, /Sector rank #1/);
assert.match(elements.stockShadowCards.innerHTML, /Ranking source/);
for (const asset of WATCHLIST) {
  assert.match(elements.stockShadowCards.innerHTML, new RegExp(`<h3>${asset.symbol}</h3>`));
}
assert.match(elements.stockShadowCards.innerHTML, /IT \/ Technology · Stock/);
assert.match(elements.stockShadowCards.innerHTML, /Healthcare · Stock/);
assert.match(elements.stockShadowCards.innerHTML, /No saved result yet/);
assert.match(elements.stockShadowCards.innerHTML, /0.70/);
renderStockShadow({ ...base, enabled: true });
assert.match(elements.stockShadowNote.textContent, /Waiting for the first result/);
const decision = {symbol: 'SPY', action: 'buy', score: .8, session_date: '<img src=x onerror=alert(1)>',
  close_price: 500, analyzed_at: '2026-11-27T18:20:00Z'};
renderStockShadow({ ...base, enabled: true, decisions: [decision] });
assert.match(elements.stockShadowCards.innerHTML, /Leaning toward buying/);
assert.match(elements.stockShadowCards.innerHTML, /0.800/);
assert.match(elements.stockShadowCards.innerHTML, /&lt;img/);
assert.doesNotMatch(elements.stockShadowCards.innerHTML, /<img/);
assert.match(elements.stockShadowNote.textContent, /not live prices/);
assert.doesNotMatch(elements.stockShadowCards.innerHTML, /<button/);
renderStockShadow({ ...base, decisions: [decision] });
assert.match(elements.stockShadowNote.textContent, /historical/);
elements.stockSectorFilter.value = 'Healthcare';
renderStockShadow(base);
assert.equal((elements.stockShadowCards.innerHTML.match(/<article/g) || []).length, 10);
assert.match(elements.stockShadowCards.innerHTML, /Healthcare · 10 stocks/);
assert.doesNotMatch(elements.stockShadowCards.innerHTML, /IT \/ Technology · Stock/);
assert.match(elements.stockCoverage.textContent, /0 of 10/);
renderStockShadow({ ...base, enabled: true });
assert.equal(elements.stockSectorFilter.value, 'Healthcare');
elements.stockSectorFilter.value = 'ETF benchmarks';
renderStockShadow(base);
assert.equal((elements.stockShadowCards.innerHTML.match(/<article/g) || []).length, 2);
assert.doesNotMatch(elements.stockShadowCards.innerHTML, /Sector rank/);
elements.stockSectorFilter.value = 'unknown-sector';
renderStockShadow(base);
assert.equal(elements.stockSectorFilter.value, '');
assert.equal((elements.stockShadowCards.innerHTML.match(/<article/g) || []).length, 102);
(async () => {
  await assert.rejects(loadStockShadow(), /offline/);
  assert.match(elements.stockShadowNote.textContent, /previously loaded snapshots/);
  assert.match(elements.stockShadowNote.className, /error/);
})().catch(error => { console.error(error); process.exitCode = 1; });
'''
    path = tmp_path / "stock-ui.cjs"
    fixtures = (f"const WATCHLIST = {json.dumps(WATCHLIST)}; const SYMBOLS = {json.dumps(SYMBOLS)}; "
                f"const RANKING = {json.dumps(RANKING)};")
    path.write_text(harness + "\n" + fixtures + "\n" + script + "\n" + checks)
    subprocess.run(["node", "--check", str(path)], check=True, capture_output=True, text=True)
    result = subprocess.run(["node", str(path)], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
