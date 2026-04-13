"""
Root conftest.py — registers pytest markers for the full test suite.

Run subsets:
    pytest tests/unit                          # fast, no Docker
    pytest tests/integration -m integration    # requires Docker
    pytest tests/chaos       -m chaos          # fast, no Docker
    pytest tests/e2e         -m e2e            # requires Docker (all services)
"""

import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "integration: marks tests that require real Docker containers (Redis, Kafka, Postgres)",
    )
    config.addinivalue_line(
        "markers",
        "chaos: marks fault-injection / resilience tests",
    )
    config.addinivalue_line(
        "markers",
        "e2e: marks full-stack end-to-end tests requiring all services",
    )
