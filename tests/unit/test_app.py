"""Tests for the FastAPI application factory."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.api.app import create_app
from src.orc.dispatch import OrcDispatch


class TestApp:
    def test_default_grpc_port_is_not_50051(self) -> None:
        app = create_app()
        assert app.state.grpc_port != 50051

    def test_health_endpoint(self) -> None:
        app = create_app()
        client = TestClient(app)
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_run_endpoint(self) -> None:
        dispatch = OrcDispatch()
        app = create_app(dispatch=dispatch)
        client = TestClient(app)
        response = client.post("/api/v1/run", json={"prompt": "hello"})
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data["session_id"], str)
        assert len(data["session_id"]) > 0
        assert data["session_id"].startswith("session-")
        assert data["turn_index"] == 1
        assert data["backend"] == "local"
        assert data["output"] == "local:1:hello"

    def test_run_endpoint_with_session_id(self) -> None:
        dispatch = OrcDispatch()
        app = create_app(dispatch=dispatch)
        client = TestClient(app)
        response = client.post("/api/v1/run", json={"prompt": "hi", "session_id": "thread-3"})
        assert response.status_code == 200
        data = response.json()
        assert data["session_id"] == "thread-3"

    def test_run_endpoint_rejects_blank_prompt(self) -> None:
        dispatch = OrcDispatch()
        app = create_app(dispatch=dispatch)
        client = TestClient(app)
        response = client.post("/api/v1/run", json={"prompt": ""})
        assert response.status_code == 422

    def test_run_endpoint_rejects_missing_prompt(self) -> None:
        dispatch = OrcDispatch()
        app = create_app(dispatch=dispatch)
        client = TestClient(app)
        response = client.post("/api/v1/run", json={})
        assert response.status_code == 422

    def test_run_endpoint_with_metadata(self) -> None:
        dispatch = OrcDispatch()
        app = create_app(dispatch=dispatch)
        client = TestClient(app)
        response = client.post(
            "/api/v1/run",
            json={"prompt": "test", "metadata": {"env": "prod"}},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["turn_index"] == 1

    def test_multiple_runs_increment_turn_index(self) -> None:
        dispatch = OrcDispatch()
        app = create_app(dispatch=dispatch)
        client = TestClient(app)

        r1 = client.post("/api/v1/run", json={"prompt": "first", "session_id": "s1"})
        r2 = client.post("/api/v1/run", json={"prompt": "second", "session_id": "s1"})

        assert r1.json()["turn_index"] == 1
        assert r2.json()["turn_index"] == 2

    def test_custom_dispatch_injection(self) -> None:
        dispatch = OrcDispatch()
        app = create_app(dispatch=dispatch)
        client = TestClient(app)
        response = client.get("/health")
        assert response.status_code == 200
