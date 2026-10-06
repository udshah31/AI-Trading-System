# Deploying to Oracle Cloud (Always Free)

The whole stack (dashboard, trading system, Postgres, Redis, Prometheus, Grafana) runs with
`docker compose` on **one Always Free Ampere A1 VM**. On every push to `main`, the single CI
runner runs the tests, lint and type check, then—if those pass—SSHes in and runs
[`scripts/deploy.sh`](scripts/deploy.sh): it checks out that exact commit, rebuilds on the VM
(native ARM), restarts in place and health-checks. Keeping deployment in the same job avoids
waiting for a second GitHub-hosted runner. There is only ever one bot. Its positions survive
restarts (Redis AOF).

Only SSH (port 22) is open to the internet. You reach the dashboard and Grafana through an
SSH tunnel, so the basic-auth password never crosses the internet unencrypted.

## Local development and CI checks

Use **Python 3.11** (`.python-version`), matching the container images and CI. Run these
commands from the repository root. They install dependencies and run mocked/local tests;
they do **not** start a trading process or require broker/LLM credentials.

### Create the isolated environment

With `uv` installed:

```bash
git submodule update --init --recursive TradingAgents
# Downloads a uv-managed Python 3.11 if missing; does not change the global interpreter.
uv venv --python 3.11 --managed-python .venv
uv pip install --python .venv/bin/python -c constraints.txt -r requirements-dev.txt
uv pip check --python .venv/bin/python
```

Alternatively, if Python 3.11 is already installed, use standard `venv` and `pip` in a
fresh checkout instead of the `uv` commands:

```bash
git submodule update --init --recursive TradingAgents
python3.11 -m venv .venv
.venv/bin/python -m pip install -c constraints.txt -r requirements-dev.txt
.venv/bin/python -m pip check
```

`TradingAgents` is installed in editable mode from the **commit pinned by the Git
submodule**, not from a global installation or the latest upstream branch. Do not use
`git submodule update --remote` for normal setup. The `.venv` and check caches are ignored
by Git. Activate it with `source .venv/bin/activate`, or use the explicit paths below.

### Run the same checks as CI

```bash
.venv/bin/python -m pytest tests/ -v -p no:anchorpy -q
.venv/bin/python -m ruff check hybrid/ --select=E9,F63,F7,F82
.venv/bin/python -m mypy hybrid/ --ignore-missing-imports
```

Keep `-p no:anchorpy`: the installed AnchorPy package registers a pytest plugin that
these tests do not use. A missing `alpaca-py` silently skips the broker test module;
the complete development requirements include it, so that module should run.

These checks do not verify live broker behavior, LLM API calls, or a deployed stack.
Building/running the Compose stack additionally requires Docker Engine and the **Docker
Compose plugin** (`docker compose version`); the Python environment does not provide those.

### Dependency versions

- `requirements.txt` and `requirements-dex.txt` declare runtime dependencies.
- `requirements-dev.txt` adds the pinned submodule and all test/lint/typecheck tools.
- `constraints.txt` pins resolved dependency versions, including platform markers.
  Local setup, both CI workflows, and both Dockerfiles use these constraints. A constraint
  does not install an otherwise-unused package, so containers do not gain the dev tools.

For an intentional dependency update, regenerate the constraints and rerun all checks:

```bash
uv pip compile requirements-dev.txt --python-version 3.11 --universal \
  --no-emit-package tradingagents --output-file constraints.txt
uv pip install --python .venv/bin/python -c constraints.txt -r requirements-dev.txt
uv pip check --python .venv/bin/python
```

TradingAgents itself is omitted from the constraints because its source is pinned by Git;
its dependencies are included. Cross-platform resolution is not a substitute for building
and testing on the target OS. Only deploy after the CI checks and the deployment checks pass.

## Optional stock shadow research (top ten per sector plus ETFs)

The watchlist includes SPY/QQQ plus **100 individual stocks**: ten US-listed companies
ranked by market capitalization within each of these ten categories: IT/technology,
healthcare, financials, consumer staples, consumer discretionary, industrials, energy,
utilities, real estate and communication services. Materials is not included in these
previously selected categories. Market capitalization measures size, not investment quality.
This is not a recommendation, predicted-return ranking, or sector-balanced portfolio.
ETF holdings can overlap with the individual companies.

The reviewed snapshot is `hybrid/stock_watchlist.json`. It includes names, source-sector
classification, numeric market caps, per-sector ranks, source URLs, retrieval timestamp,
and selection methodology (including US-listed foreign companies/ADRs and single-share-class
handling). Source quotes may be delayed; retrieval time does not imply all values share
that valuation timestamp. The snapshot **does not automatically re-rank**. Updating it
requires a reviewed data change and deployment. The loader verifies ten companies per sector,
unique tickers, contiguous ranks and descending positive market caps.

The dashboard uses one category-based research table. It lists all eleven groups (ten sectors
plus ETF benchmarks) as expandable summary rows with asset counts, saved-result coverage,
signal counts, and median scores, followed by the matching stock rows. These summaries cover
the full watchlist before filters, use each symbol's latest saved session, and do not represent
same-day sector performance. Missing/invalid scores are excluded from the median. The table
supports search by ticker/company/sector, category and signal filters, and expand/collapse
controls. Rows show stock/company, decision,
last adjusted close, research session, and expandable evidence. Decisions use the saved
analysis action: Buy signal, Sell signal, or Wait. Symbols without results show Waiting for
data; unrecognized actions show Unknown signal. Row details include score, strategy conviction
(not a profit probability), recorded time, saved thresholds, technical inputs/weights,
source links, market capitalization and sector rank. Older rows without evidence show unavailable;
current thresholds are clearly reference-only when saved thresholds are absent.
Watchlist methodology is under Advanced details. Ranking retrieval
and research session dates are separate. Coverage counts show how many displayed symbols
have a saved result. Category groups start collapsed on mobile; mobile rows stack their fields.
Category expansion state, expanded row details and keyboard focus survive a successful refresh;
failed-refresh warnings remain visible through navigation and filtering until recovery.
Price-data failures do not replace the sourced ranking with guessed alternatives.
Existing results are retained; the latest saved row is selected independently per symbol.

Stock shadow research is **off by default**. Enable it with `STOCK_SHADOW_ENABLED=true`
in the deployment `.env`. It requires `BROKER=alpaca`, valid Alpaca paper credentials for
calendar access, and working database storage. Set the flag for both application services
(the Compose file already forwards it to the bot and dashboard).

A failed stock-data request is skipped without blocking the other sectors, and retried
on a later check. The dashboard shows the latest saved snapshot for each symbol independently.

The bot checks every five minutes and selects the latest exchange session that closed at
least 20 minutes ago. Alpaca's calendar supplies holidays, early closes and exchange-local
session times; timezone conversion handles daylight saving. Each symbol/session is saved
once in `stock_shadow_decisions`, with a database uniqueness constraint that survives
restarts. On first enable/startup it can catch up the last completed session.

Only valid completed daily bars are accepted; missing/stale/non-finite data is skipped and
retried. This version uses **technical-only signals and Yahoo adjusted daily closes**, not
tradable execution quotes. It records the technical weights used. No stock LLM requests,
risk approvals, broker orders, or stock outcomes for the crypto learning loop are generated.
The existing BTC/ETH schedule and capital are unchanged.

The dashboard's beginner-friendly **Stock decisions** table under **Stock research** and authenticated
`GET /api/stocks/shadow` endpoint show the latest persisted results. The enabled indicator
reflects the configuration flag, not proof of healthy calendar/data access; inspect bot logs
for `[StockShadow]` failures and the displayed session dates for freshness. Historical rows
remain visible when the feature is disabled. A Buy signal / Sell signal label here is research,
**not an executed trade or an instruction to trade**.

Test locally without API calls: `.venv/bin/python -m pytest tests/test_stock_shadow.py -q -p no:anchorpy`.
Enabling this feature on Oracle requires a separate reviewed deployment; do not restart the
production stack solely to test local changes.

## 1. Create the VM (once)

1. OCI Console → Compute → Instances → **Create instance**.
2. Image: **Canonical Ubuntu** (22.04 or 24.04). Shape: **VM.Standard.A1.Flex** (Ampere,
   Always Free eligible), e.g. 2 OCPU / 12 GB. If you see "Out of host capacity", try another
   availability domain or retry later.
3. Add your personal SSH public key. Keep the default VCN; its security list allows only port 22.

Oracle may reclaim Always Free instances that sit idle for long periods. A running bot
normally isn't idle, but check the OCI console if the VM disappears.

## 2. Prepare the VM (once)

```bash
ssh ubuntu@<vm-ip>
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker ubuntu && exit            # log in again for the group to apply
ssh ubuntu@<vm-ip>
git clone --recurse-submodules https://github.com/udshah31/AI-Trading-System.git
cd AI-Trading-System
cp .env.template .env && nano .env                # DASHBOARD_PASSWORD, GRAFANA_PASSWORD, API keys
docker compose up -d --build                      # first run by hand
```

With the default `BROKER=alpaca`, orders go to your **Alpaca paper account**: set `ALPACA_API_KEY` and `ALPACA_SECRET_KEY` to your paper keys and leave `LIVE_TRADING` unset. Check that Alpaca offers crypto in your state.

## 3. Connect CI (once)

```bash
# on your machine: a key pair used only for deploys
ssh-keygen -t ed25519 -f deploy_key -N "" -C github-deploy
ssh-copy-id -i deploy_key.pub ubuntu@<vm-ip>
ssh-keyscan -H <vm-ip>                            # output -> DEPLOY_KNOWN_HOSTS
```

In GitHub, add the repository secrets `DEPLOY_HOST`, `DEPLOY_USER`, `DEPLOY_SSH_KEY` (the
contents of `deploy_key`) and `DEPLOY_KNOWN_HOSTS`. Details are in
[.github/SECRETS.md](.github/SECRETS.md). Delete the local `deploy_key` once it's saved in
GitHub. The deployment command runs only on pushes to `main`, after all checks pass.

## Using it

```bash
ssh -L 8000:localhost:8000 -L 3000:localhost:3000 ubuntu@<vm-ip>
# then open http://localhost:8000 (dashboard) and http://localhost:3000 (Grafana)
```

- **Logs:** `docker compose logs -f trading-system`
- **Deploy by hand:** `bash scripts/deploy.sh $(git rev-parse origin/main)` on the VM, after `git fetch`.
- **Going live:** set `LIVE_TRADING=true` in the VM's `.env`, then `docker compose up -d`.
- **Public dashboard:** set `DASHBOARD_BIND=0.0.0.0` only behind a TLS reverse proxy (for
  example Caddy with a domain name), and open the port in the VCN security list.
