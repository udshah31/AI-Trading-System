# Deploying to Oracle Cloud (Always Free)

The whole stack (dashboard, trading system, Postgres, Redis, Prometheus, Grafana) runs with
`docker compose` on **one Always Free Ampere A1 VM**. On every push to `main` that passes
tests, CI waits for your approval, then SSHes in and runs [`scripts/deploy.sh`](scripts/deploy.sh):
it checks out that exact commit, rebuilds on the VM (native ARM), restarts in place and
health-checks. There is only ever one bot. Its positions survive restarts (Redis AOF).

Only SSH (port 22) is open to the internet. You reach the dashboard and Grafana through an
SSH tunnel, so the basic-auth password never crosses the internet unencrypted.

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

In GitHub, add the secrets `DEPLOY_HOST`, `DEPLOY_USER`, `DEPLOY_SSH_KEY` (the contents of
`deploy_key`) and `DEPLOY_KNOWN_HOSTS`. Then create the `production` environment with yourself as
required reviewer. Details are in [.github/SECRETS.md](.github/SECRETS.md). Delete the local
`deploy_key` once it's saved in GitHub.

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
