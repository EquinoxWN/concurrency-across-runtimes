"""Shared scenario parameters (spec/scenarios.json), the same file Java and JS read."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

SPEC = Path(__file__).resolve().parents[2] / "spec" / "scenarios.json"


@pytest.fixture(scope="session")
def scenarios() -> dict[str, Any]:
    """Parsed scenarios.json."""
    data: dict[str, Any] = json.loads(SPEC.read_text(encoding="utf-8"))
    return data
