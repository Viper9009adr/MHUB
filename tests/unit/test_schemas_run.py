"""Tests for the run request/response schemas."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.api.schemas.run import ErrorResponse, HealthResponse, RunRequest, RunResponse


class TestRunRequest:
    def test_valid_minimal_request(self) -> None:
        req = RunRequest(prompt="hello")
        assert req.prompt == "hello"
        assert req.session_id is None
        assert req.metadata == {}

    def test_valid_full_request(self) -> None:
        req = RunRequest(
            prompt="run analysis",
            session_id="sess-1",
            metadata={"key": "value"},
        )
        assert req.prompt == "run analysis"
        assert req.session_id == "sess-1"
        assert req.metadata == {"key": "value"}

    def test_rejects_blank_prompt(self) -> None:
        with pytest.raises(ValidationError):
            RunRequest(prompt="")

    def test_rejects_missing_prompt(self) -> None:
        with pytest.raises(ValidationError):
            RunRequest()  # type: ignore[call-arg]

    def test_metadata_defaults_to_empty_dict(self) -> None:
        req = RunRequest(prompt="test")
        assert req.metadata == {}


class TestRunResponse:
    def test_valid_response(self) -> None:
        resp = RunResponse(
            session_id="sess-1",
            turn_index=1,
            backend="local",
            output="local:1:hello",
        )
        assert resp.session_id == "sess-1"
        assert resp.turn_index == 1
        assert resp.backend == "local"
        assert resp.output == "local:1:hello"


class TestHealthResponse:
    def test_default_status(self) -> None:
        resp = HealthResponse()
        assert resp.status == "ok"


class TestErrorResponse:
    def test_error_detail(self) -> None:
        err = ErrorResponse(detail="something went wrong")
        assert err.detail == "something went wrong"
