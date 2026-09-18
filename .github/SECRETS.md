# GitHub Secrets Required for CI/CD

## Oracle Cloud Infrastructure (OCI) - Required for Deploy

| Secret Name | Description | How to Get |
|-------------|-------------|------------|
| `OCI_USER_OCID` | OCID of the API user | Identity > Users > API Keys |
| `OCI_FINGERPRINT` | API key fingerprint | Identity > Users > API Keys > Fingerprint |
| `OCI_TENANCY_OCID` | Tenancy OCID | Administration > Tenancy Details |
| `OCI_REGION` | Home region (e.g., `us-phoenix-1`) | Profile > Regions |
| `OCI_PRIVATE_KEY` | Private key PEM content | Generated with API key pair |
| `OCI_TENANCY_NAMESPACE` | Object storage namespace | Object Storage > Namespace |
| `OCI_COMPARTMENT_OCID` | Compartment OCID | Identity > Compartments |
| `OCI_SUBNET_OCID` | Subnet OCID for container | Networking > Subnets |
| `OCI_USERNAME` | Auth token username | User Settings > Auth Tokens |
| `OCI_AUTH_TOKEN` | Auth token password | User Settings > Auth Tokens |

## Application Secrets - Required for Runtime

| Secret Name | Description |
|-------------|-------------|
| `DATABASE_URL` | PostgreSQL connection string (e.g., `postgresql+asyncpg://user:pass@host:5432/db`) |
| `REDIS_URL` | Redis connection string (e.g., `redis://:pass@host:6379/0`) |
| `KRAKEN_API_KEY` | Kraken Pro API key |
| `KRAKEN_API_SECRET` | Kraken Pro API secret |
| `GOOGLE_API_KEY` | Google Gemini API key |
| `SENTRY_ACCESS_TOKEN` | Sentry auth token for error tracking |
| `EXA_API_KEY` | Exa.ai API key for web search |
| `BROWSER_USE_API_KEY` | Browser Use Cloud API key |
| `ANTHROPIC_API_KEY` | Anthropic API key |
| `OPENAI_API_KEY` | OpenAI API key |
| `OPENROUTER_API_KEY` | OpenRouter API key |
| `EMBEDDED_AGENT_PROVIDER` | LLM provider: `openai` / `anthropic` / `openrouter` |
| `SENTRY_ACCESS_TOKEN` | Sentry auth token (duplicate of above) |

## Optional

| Secret Name | Description |
|-------------|-------------|
| `SLACK_WEBHOOK_URL` | Slack webhook for failure notifications |
| `SOLANA_PRIVATE_KEY` | Solana private key for DEX sniper |
| `SOLANA_RPC_URL` | Solana RPC endpoint (e.g., Helius) |
| `BASE_RPC_URL` | Base RPC endpoint |
| `EVM_PRIVATE_KEY` | EVM private key for DEX sniper |
| `BASE_RPC_URL` | Base RPC URL |
| `ARBITRUM_RPC_URL` | Arbitrum RPC URL |
| `ETHEREUM_RPC_URL` | Ethereum RPC URL |

---

## How to Add Secrets

1. Go to **GitHub Repository** → **Settings** → **Secrets and variables** → **Actions**
2. Click **New repository secret**
3. Add each secret with exact name (case-sensitive)

---

## Oracle Cloud Setup Steps

1. **Create OCI User & API Key**
   ```
   # Generate key pair
   openssl genrsa -out oci_api_key.pem 2048
   openssl rsa -pubout -in oci_api_key.pem -out oci_api_key_public.pem
   ```

2. **Add API Key to OCI User**
   - Console → Identity > Users > [Your User] > API Keys
   - Add Public Key → paste `oci_api_key_public.pem` content
   - Note the **Fingerprint** shown

3. **Create Auth Token** (for registry login)
   - User Settings > Auth Tokens > Generate Token
   - Save as `OCI_AUTH_TOKEN`

4. **Get Compartment & Subnet OCIDs**
   - Identity > Compartments → Copy OCID
   - Networking > VCN > Subnets → Copy Subnet OCID

5. **Create Registry**
   - Artifacts & Registry > Container Registry > Create Repository
   - Note: `${region}.ocir.io/${namespace}/ai-trading-system`

---

## Local Testing

```bash
# Test OCI CLI locally
oci --version

# Test auth
oci iam user get --user-id $OCI_USER_OCID

# Test registry login
docker login ${OCI_REGION}.ocir.io -u ${OCI_USERNAME} -p ${OCI_AUTH_TOKEN}

# Test deploy locally
oci container-instances container-instance create --help
```