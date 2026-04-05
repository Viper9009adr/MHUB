"""Repository-local sensitive data scanner with safe redacted output."""

from __future__ import annotations

import argparse
import hashlib
import math
import re
from dataclasses import dataclass
from pathlib import Path


SKIP_DIRS = {
    ".git",
    "node_modules",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".hypothesis",
    "build",
    "dist",
    "artifacts",
}

ALLOWED_TEXT_EXT = {
    ".py",
    ".ts",
    ".tsx",
    ".js",
    ".jsx",
    ".json",
    ".yaml",
    ".yml",
    ".toml",
    ".ini",
    ".cfg",
    ".conf",
    ".env",
    ".md",
    ".txt",
    ".log",
    ".sql",
    ".sh",
}

VERIFY_DENYLIST = {
    "example",
    "changeme",
    "your_",
    "test",
    "dummy",
    "sample",
    "placeholder",
    "xxxxxxxx",
}


@dataclass(frozen=True)
class Rule:
    name: str
    pattern: re.Pattern[str]
    entropy_floor: float


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    kind: str
    redacted: str
    fingerprint: str


RULES = [
    Rule("private-key", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"), 0.0),
    Rule("jwt", re.compile(r"\beyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\b"), 3.4),
    Rule("bearer", re.compile(r"Bearer\s+([A-Za-z0-9._\-]{16,})", re.IGNORECASE), 3.2),
    Rule(
        "api-key",
        re.compile(
            r"(?i)\b(?:api[_-]?key|token|secret|password|passwd|db[_-]?(?:url|pass|password))\b\s*[:=]\s*['\"]?([A-Za-z0-9/+._\-]{12,})"
        ),
        3.0,
    ),
]


def shannon_entropy(value: str) -> float:
    if not value:
        return 0.0
    counts: dict[str, int] = {}
    for ch in value:
        counts[ch] = counts.get(ch, 0) + 1
    entropy = 0.0
    length = len(value)
    for count in counts.values():
        p = count / length
        entropy -= p * math.log2(p)
    return entropy


def redact(value: str) -> tuple[str, str]:
    if len(value) <= 4:
        masked = "*" * len(value)
    else:
        masked = f"{value[:2]}{'*' * (len(value) - 4)}{value[-2:]}"
    fp = hashlib.sha256(value.encode("utf-8", errors="ignore")).hexdigest()[:10]
    return masked, fp


def candidate_from_match(match: re.Match[str]) -> str:
    if match.lastindex:
        return (match.group(1) or "").strip()
    return match.group(0).strip()


def verified(secret: str, entropy_floor: float) -> bool:
    if len(secret) < 12:
        return False
    lowered = secret.lower()
    if any(token in lowered for token in VERIFY_DENYLIST):
        return False
    return shannon_entropy(secret) >= entropy_floor


def should_scan(path: Path) -> bool:
    if path.suffix.lower() in ALLOWED_TEXT_EXT:
        return True
    return path.name == ".env"


def iter_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if should_scan(path):
            files.append(path)
    return files


def scan_file(file_path: Path, root: Path) -> list[Finding]:
    findings: list[Finding] = []
    try:
        lines = file_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return findings

    rel = str(file_path.relative_to(root))
    for index, line in enumerate(lines, start=1):
        for rule in RULES:
            for match in rule.pattern.finditer(line):
                raw = candidate_from_match(match)
                if not raw:
                    continue
                if rule.name != "private-key" and not verified(raw, rule.entropy_floor):
                    continue
                masked, fp = redact(raw)
                findings.append(Finding(rel, index, rule.name, masked, fp))
    return findings


def run(root: Path) -> int:
    findings: list[Finding] = []
    for file_path in iter_files(root):
        findings.extend(scan_file(file_path, root))

    print(f"scope={root}")
    print(f"files_scanned={len(iter_files(root))}")
    print(f"findings={len(findings)}")
    for item in findings:
        print(
            " | ".join(
                [
                    f"path={item.path}",
                    f"line={item.line}",
                    f"kind={item.kind}",
                    f"redacted={item.redacted}",
                    f"fp={item.fingerprint}",
                ]
            )
        )
    if findings:
        print("policy=Detected candidate secrets; revoke and rotate exposed credentials immediately.")
        return 1
    print("policy=No verified live secrets detected in repository scope.")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan repo for hardcoded sensitive data safely.")
    parser.add_argument("--root", type=Path, default=Path.cwd(), help="Repository root to scan")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.root.resolve()
    return run(root)


if __name__ == "__main__":
    raise SystemExit(main())
