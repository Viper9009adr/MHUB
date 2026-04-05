.PHONY: dev test lint proto clean install

# =============================================================================
# Meridian HUB — Makefile
# =============================================================================

PYTHON := python3
UV := uv
PROTO_DIR := proto
PROTO_OUT := src/hub

# ---------------------------------------------------------------------------
# Install dependencies
# ---------------------------------------------------------------------------
install:
	$(UV) sync --all-extras

# ---------------------------------------------------------------------------
# Development: start the API server + gRPC daemon
# ---------------------------------------------------------------------------
dev:
	bash scripts/dev.sh

# ---------------------------------------------------------------------------
# Run test suite
# ---------------------------------------------------------------------------
test:
	$(PYTHON) -m pytest -v tests

test-cov:
	$(PYTHON) -m pytest -v --cov=src --cov-report=term-missing tests

# ---------------------------------------------------------------------------
# Linting
# ---------------------------------------------------------------------------
lint:
	$(PYTHON) -m py_compile src/agentic_cli/*.py src/api/*.py src/api/routes/*.py src/api/schemas/*.py src/hub/*.py src/orc/*.py src/storage/*.py src/llm/*.py env.py

lint-ts:
	npx tsc --noEmit --project reconstructed/tsconfig.tui.json

# ---------------------------------------------------------------------------
# Protobuf code generation
# ---------------------------------------------------------------------------
proto:
	$(PYTHON) -m grpc_tools.protoc \
		-I$(PROTO_DIR) \
		--python_out=$(PROTO_OUT) \
		--grpc_python_out=$(PROTO_OUT) \
		--pyi_out=$(PROTO_OUT) \
		$(PROTO_DIR)/hub.proto

# ---------------------------------------------------------------------------
# TUI build
# ---------------------------------------------------------------------------
build-tui:
	cd reconstructed && npx vite build

# ---------------------------------------------------------------------------
# Clean build artifacts
# ---------------------------------------------------------------------------
clean:
	rm -rf dist/ build/ *.egg-info/
	rm -rf reconstructed/dist/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyi" -not -path "./src/hub/*" -delete 2>/dev/null || true
