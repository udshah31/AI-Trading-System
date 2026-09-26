"""The committed .env.template must never carry real credentials."""
import re
from pathlib import Path

# API keys, secrets, tokens and private keys are always the user's own; local
# service passwords (DB_PASSWORD, GRAFANA_PASSWORD) may keep dev defaults.
CREDENTIAL_NAME = re.compile(r"(_API_KEY|_SECRET|_SECRET_KEY|_PRIVATE_KEY|_PASSPHRASE|_TOKEN)$")


def test_env_template_has_no_credential_values():
    filled = []
    for lineno, line in enumerate(Path(".env.template").read_text().splitlines(), 1):
        name, sep, value = line.partition("=")
        if sep and not line.lstrip().startswith("#") and CREDENTIAL_NAME.search(name.strip()):
            if value.strip():
                filled.append(f".env.template:{lineno} {name.strip()}")
    assert not filled, f"credentials must be blank in the template: {filled}"
