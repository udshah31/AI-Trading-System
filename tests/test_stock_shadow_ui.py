"""Execute the real dashboard renderer with a minimal DOM; no browser/network required."""
import re
import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js required for dashboard renderer checks")
def test_stock_cards_and_beginner_states(tmp_path):
    page = Path("hybrid/dashboard/index.html").read_text()
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
const base = { enabled: false, decisions: [], buy_threshold: .7, sell_threshold: .3 };
renderStockShadow(base);
assert.match(elements.stockShadowNote.textContent, /Research is off/);
assert.match(elements.stockShadowCards.innerHTML, /S&P 500 ETF/);
assert.match(elements.stockShadowCards.innerHTML, /Nasdaq-100 ETF/);
assert.equal((elements.stockShadowCards.innerHTML.match(/<article/g) || []).length, 2);
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
(async () => {
  await assert.rejects(loadStockShadow(), /offline/);
  assert.match(elements.stockShadowNote.textContent, /previously loaded snapshots/);
  assert.match(elements.stockShadowNote.className, /error/);
})().catch(error => { console.error(error); process.exitCode = 1; });
'''
    path = tmp_path / "stock-ui.cjs"
    path.write_text(harness + "\n" + script + "\n" + checks)
    subprocess.run(["node", "--check", str(path)], check=True, capture_output=True, text=True)
    result = subprocess.run(["node", str(path)], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
