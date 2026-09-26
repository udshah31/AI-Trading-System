"""docker-compose exposure: only the authenticated dashboard listens beyond localhost,
and Grafana has no default admin password."""
from pathlib import Path

import yaml

COMPOSE = yaml.safe_load(Path("docker-compose.yml").read_text())
PUBLIC_SERVICES = {"dashboard"}  # basic auth, fail-closed (hybrid/dashboard/app.py)


def test_only_dashboard_is_published_beyond_localhost():
    exposed = []
    for name, service in COMPOSE["services"].items():
        for mapping in service.get("ports", []):
            if name not in PUBLIC_SERVICES and not str(mapping).startswith("127.0.0.1:"):
                exposed.append(f"{name}: {mapping}")
    assert not exposed, f"bind to 127.0.0.1 or add auth: {exposed}"


def test_grafana_password_has_no_default():
    password = COMPOSE["services"]["grafana"]["environment"]["GF_SECURITY_ADMIN_PASSWORD"]
    assert password.startswith("${GRAFANA_PASSWORD:?"), password  # compose errors when unset


def test_env_template_leaves_grafana_password_blank():
    lines = Path(".env.template").read_text().splitlines()
    assert "GRAFANA_PASSWORD=" in lines
