"""docker-compose exposure: nothing is published beyond localhost by default (the
dashboard can opt in with DASHBOARD_BIND), and Grafana has no default admin password."""
from pathlib import Path

import yaml

COMPOSE = yaml.safe_load(Path("docker-compose.yml").read_text())
# Only the dashboard (basic auth, fail-closed) may be opened up, and only by setting DASHBOARD_BIND
LOCALHOST_DEFAULTS = {"127.0.0.1:", "${DASHBOARD_BIND:-127.0.0.1}:"}


def test_nothing_published_beyond_localhost_by_default():
    exposed = []
    for name, service in COMPOSE["services"].items():
        for mapping in service.get("ports", []):
            if not any(str(mapping).startswith(prefix) for prefix in LOCALHOST_DEFAULTS):
                exposed.append(f"{name}: {mapping}")
            elif name != "dashboard" and not str(mapping).startswith("127.0.0.1:"):
                exposed.append(f"{name}: {mapping} (only the dashboard may be configurable)")
    assert not exposed, f"bind to 127.0.0.1: {exposed}"


def test_prometheus_scrapes_dashboard_over_compose_network():
    prometheus = yaml.safe_load(Path("prometheus.yml").read_text())
    [job] = [j for j in prometheus["scrape_configs"] if j["job_name"] == "dashboard"]
    assert job["static_configs"][0]["targets"] == ["dashboard:8000"]


def test_grafana_password_has_no_default():
    password = COMPOSE["services"]["grafana"]["environment"]["GF_SECURITY_ADMIN_PASSWORD"]
    assert password.startswith("${GRAFANA_PASSWORD:?"), password  # compose errors when unset


def test_env_template_leaves_grafana_password_blank():
    lines = Path(".env.template").read_text().splitlines()
    assert "GRAFANA_PASSWORD=" in lines
