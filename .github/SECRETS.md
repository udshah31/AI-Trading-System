# GitHub Secrets for CI/CD

Deploys go to one Oracle Cloud **Always Free** VM over SSH (see [DEPLOY.md](../DEPLOY.md)).
CI only needs SSH access. Application secrets (Kraken, LLM, TypeSafe, dashboard and
Grafana passwords, `LIVE_TRADING`) live in the VM's `.env` and are **never** stored in GitHub.

## Deploy (Settings → Secrets and variables → Actions)

| Secret | What it is |
|--------|------------|
| `DEPLOY_HOST` | Public IP or hostname of the VM |
| `DEPLOY_USER` | SSH user on the VM (e.g. `ubuntu`) |
| `DEPLOY_SSH_KEY` | Private key of a key pair used **only** for deploys; its public key is in the VM's `~/.ssh/authorized_keys` |
| `DEPLOY_KNOWN_HOSTS` | The VM's host key, from `ssh-keyscan -H <host>`; CI refuses to deploy to any other host |

Without `DEPLOY_HOST` / `DEPLOY_USER` the production deploy step is skipped with a notice,
so CI stays green. The deploy step runs in the same CI job after tests, lint and type checks;
it runs only for pushes to `main`.

## Optional

| Secret | What it is |
|--------|------------|
| `SLACK_WEBHOOK_URL` | Slack webhook for failure notifications |
