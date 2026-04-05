"""HAL (Hallucination Scanner) Agent.

Subscribes to Redis channels, analyses run results for hallucinations
using an LLM provider, and publishes reports back to Redis.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from src.llm.provider import LLMProvider
from src.orc.hal_models import HallucinationReport, Span
from src.orc.redis_bridge import RedisBridge

logger = logging.getLogger(__name__)

# Channel names
CHANNEL_RESULTS = "orc:results:*"
CHANNEL_REPORTS = "orc:hal:reports"


class HalAgent:
    """HallucinationScannerAgent.

    Listens for run results on Redis, sends them to an LLM for
    hallucination analysis, and publishes structured reports.

    Args:
        llm_provider: LLM provider for hallucination analysis.
        llm_model: Model to use for analysis.
        redis: RedisBridge for pub/sub communication.
    """

    def __init__(
        self,
        llm_provider: LLMProvider,
        llm_model: str,
        redis: RedisBridge | None = None,
    ) -> None:
        self._llm_provider = llm_provider
        self._llm_model = llm_model
        self._redis = redis

    async def analyse(self, run_id: str, prompt: str, response: str) -> HallucinationReport:
        """Analyse a run result for hallucinations.

        Args:
            run_id: The unique run identifier.
            prompt: The original prompt.
            response: The LLM response to analyse.

        Returns:
            HallucinationReport with findings.
        """
        analysis_prompt = (
            f"Analyse the following prompt/response pair for hallucinations, "
            f"factual errors, or unsupported claims.\n\n"
            f"PROMPT: {prompt}\n\n"
            f"RESPONSE: {response}\n\n"
            f"Return a JSON object with fields: "
            f'"hallucination_score" (0.0-1.0), '
            f'"spans" (list of {{"start": int, "end": int, "text": str, "issue": str}}), '
            f'"summary" (str).'
        )

        try:
            result = await self._llm_provider.complete(
                prompt=analysis_prompt,
                model=self._llm_model,
                temperature=0.0,
            )
        except Exception as exc:
            logger.error("HAL analysis failed for run %s: %s", run_id, exc)
            return HallucinationReport(
                run_id=run_id,
                hallucination_score=0.0,
                spans=[],
                summary=f"Analysis failed: {exc}",
            )

        return self._parse_report(run_id, result.text)

    async def publish_report(self, report: HallucinationReport) -> None:
        """Publish a hallucination report to Redis.

        Args:
            report: The report to publish.
        """
        if self._redis is None:
            logger.debug("Redis not available, skipping report publish")
            return

        channel = f"orc:hal:reports:{report.run_id}"
        payload = report.model_dump_json()
        try:
            await self._redis.publish(channel, payload.encode("utf-8"))
            logger.info("HAL report published for run %s", report.run_id)
        except Exception as exc:
            logger.error("Failed to publish HAL report for run %s: %s", report.run_id, exc)

    def _parse_report(self, run_id: str, text: str) -> HallucinationReport:
        """Parse LLM response into a HallucinationReport.

        Args:
            run_id: The run identifier.
            text: Raw LLM response text (expected JSON).

        Returns:
            Parsed HallucinationReport.
        """
        try:
            data = json.loads(text)
            spans = []
            for span_data in data.get("spans", []):
                spans.append(Span(
                    start=span_data.get("start", 0),
                    end=span_data.get("end", 0),
                    text=span_data.get("text", ""),
                    issue=span_data.get("issue", ""),
                ))
            return HallucinationReport(
                run_id=run_id,
                hallucination_score=float(data.get("hallucination_score", 0.0)),
                spans=spans,
                summary=data.get("summary", ""),
            )
        except (json.JSONDecodeError, ValueError, TypeError) as exc:
            logger.warning("Failed to parse HAL report JSON: %s", exc)
            return HallucinationReport(
                run_id=run_id,
                hallucination_score=0.0,
                spans=[],
                summary=f"Parse error: {exc}",
            )


__all__ = ["HalAgent"]
