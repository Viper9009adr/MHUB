# pybatch4 Phase 4 Execution Evidence
Generated: 2026-03-30

## Summary Table

| Target | Command | Status | Exit Code |
|---|---|---|---|
| tests/unit/test_cli_main_invalid_invocation.py | .venv/bin/python -m pytest tests/unit/test_cli_main_invalid_invocation.py -v | PASSED | 0 |
| tests/pt/test_replay_sla_cli.py | .venv/bin/python -m pytest tests/pt/test_replay_sla_cli.py -v | PASSED | 0 |
| src/agentic_cli/cli.py | python3 -m py_compile src/agentic_cli/cli.py | PASSED | 0 |
| src/agentic_cli/replay_sla.py | python3 -m py_compile src/agentic_cli/replay_sla.py | PASSED | 0 |

---

## Command 1: Unit Tests — Invalid Invocation Matrix

```
cd Meridian-HUB && .venv/bin/python -m pytest tests/unit/test_cli_main_invalid_invocation.py -v 2>&1
```

**Output:**
```
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.0.2, pluggy-1.6.0 -- /home/viper9009adr/Dev/opencode-dev/Meridian-HUB/.venv/bin/python
cachedir: .pytest_cache
rootdir: /home/viper9009adr/Dev/opencode-dev/Meridian-HUB
collecting ... collected 4 items

tests/unit/test_cli_main_invalid_invocation.py::test_main_returns_two_for_unknown_subcommand PASSED [ 25%]
tests/unit/test_cli_main_invalid_invocation.py::test_main_accepts_argv_prefixed_with_program_name PASSED [ 50%]
tests/unit/test_cli_main_invalid_invocation.py::test_main_accepts_windows_style_prefixed_program_name PASSED [ 75%]
tests/unit/test_cli_main_invalid_invocation.py::test_main_accepts_windows_exe_prefixed_program_name_case_insensitive PASSED [100%]

============================== 4 passed in 0.01s ==============================
```

**Result: 4 passed, 0 failed — exit code 0**

---

## Command 2: PT Tests — Replay SLA CLI

```
cd Meridian-HUB && .venv/bin/python -m pytest tests/pt/test_replay_sla_cli.py -v 2>&1
```

**Output:**
```
============================= test session starts ==============================
platform linux -- Python 3.12.3, pytest-9.0.2, pluggy-1.6.0 -- /home/viper9009adr/Dev/opencode-dev/Meridian-HUB/.venv/bin/python
cachedir: .pytest_cache
rootdir: /home/viper9009adr/Dev/opencode-dev/Meridian-HUB
collecting ... collected 9 items

tests/pt/test_replay_sla_cli.py::test_replay_sla_cli_returns_zero_within_sla PASSED [ 11%]
tests/pt/test_replay_sla_cli.py::test_replay_sla_cli_returns_one_for_sla_breach PASSED [ 22%]
tests/pt/test_replay_sla_cli.py::test_replay_sla_cli_returns_two_for_invalid_args PASSED [ 33%]
tests/pt/test_replay_sla_cli.py::test_replay_sla_cli_handles_parse_errors_without_exiting PASSED [ 44%]
tests/pt/test_replay_sla_cli.py::test_replay_sla_cli_requires_subcommand PASSED [ 55%]
tests/pt/test_replay_sla_cli.py::test_replay_sla_cli_help_returns_zero PASSED [ 66%]
tests/pt/test_replay_sla_cli.py::test_replay_sla_cli_help_returns_zero_with_program_name_prefix PASSED [ 77%]
tests/pt/test_replay_sla_cli.py::test_replay_sla_cli_accepts_windows_style_program_name_prefix PASSED [ 88%]
tests/pt/test_replay_sla_cli.py::test_replay_sla_cli_accepts_windows_exe_program_name_prefix_case_insensitive PASSED [100%]

============================== 9 passed in 0.02s ==============================
```

**Result: 9 passed, 0 failed — exit code 0**

---

## Command 3: Compile Checks

```
cd Meridian-HUB && python3 -m py_compile src/agentic_cli/cli.py src/agentic_cli/replay_sla.py 2>&1
```

**Output:** (no output — clean compilation)

**Result: exit code 0**
