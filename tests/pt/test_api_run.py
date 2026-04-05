"""Property-based tests for the /api/v1/run endpoint."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from hypothesis import given, settings
from hypothesis import strategies as st

from src.api.app import create_app


class TestApiRunProperties:
    @given(prompt=st.text(min_size=1, max_size=100).filter(lambda s: s.strip()))
    @settings(max_examples=20)
    def test_run_accepts_any_non_empty_prompt(self, prompt: str) -> None:
        app = create_app()
        client = TestClient(app)
        response = client.post("/api/v1/run", json={"prompt": prompt})
        assert response.status_code == 200
        data = response.json()
        assert data["output"].endswith(f":{prompt.strip()}")

    @given(session_id=st.text(min_size=1, max_size=50, alphabet=st.characters(min_codepoint=32, max_codepoint=126)).filter(lambda s: s.strip()))
    @settings(max_examples=20)
    def test_run_preserves_session_id(self, session_id: str) -> None:
        app = create_app()
        client = TestClient(app)
        response = client.post(
            "/api/v1/run",
            json={"prompt": "test", "session_id": session_id},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["session_id"] == session_id.strip()

    def test_run_always_returns_consistent_backend(self) -> None:
        app = create_app()
        client = TestClient(app)
        for _ in range(5):
            response = client.post("/api/v1/run", json={"prompt": "check"})
            assert response.json()["backend"] == "local"
