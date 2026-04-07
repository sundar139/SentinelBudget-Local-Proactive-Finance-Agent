from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from sentinelbudget.review.dedup import (
    build_evidence_signature,
    build_insight_fingerprint,
    build_time_bucket,
)
from sentinelbudget.review.models import InsightDraft, ReviewFinding, ReviewMode
from sentinelbudget.review.templates import render_title_body


class InsightSummarizer(Protocol):
    def summarize(
        self,
        finding: ReviewFinding,
        fallback_title: str,
        fallback_body: str,
    ) -> tuple[str, str]:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class DeterministicTemplateSummarizer:
    def summarize(
        self,
        finding: ReviewFinding,
        fallback_title: str,
        fallback_body: str,
    ) -> tuple[str, str]:
        del finding
        return fallback_title, fallback_body


@dataclass(slots=True)
class InsightGenerator:
    summarizer: InsightSummarizer = DeterministicTemplateSummarizer()

    def build_drafts(
        self,
        user_id: UUID,
        findings: list[ReviewFinding],
        mode: ReviewMode,
        generated_at: datetime,
    ) -> list[InsightDraft]:
        drafts: list[InsightDraft] = []

        for finding in findings:
            fallback_title, fallback_body = render_title_body(finding)
            try:
                title, body = self.summarizer.summarize(
                    finding,
                    fallback_title,
                    fallback_body,
                )
            except Exception:
                title, body = fallback_title, fallback_body

            if title.strip() == "":
                title = fallback_title
            if body.strip() == "":
                body = fallback_body

            evidence_signature = build_evidence_signature(finding.evidence)
            time_bucket = build_time_bucket(mode, generated_at)
            fingerprint = build_insight_fingerprint(
                user_id=user_id,
                kind=finding.kind,
                severity=finding.severity,
                evidence_signature=evidence_signature,
                time_bucket=time_bucket,
            )

            details = {
                "finding_kind": finding.kind,
                "finding_summary": finding.summary,
                "severity": finding.severity,
                "evidence": finding.evidence,
                "evidence_signature": evidence_signature,
                "time_bucket": time_bucket,
                "review_mode": mode,
            }

            drafts.append(
                InsightDraft(
                    kind=finding.kind,
                    severity=finding.severity,
                    title=title,
                    body=body,
                    details=details,
                    fingerprint=fingerprint,
                )
            )

        return drafts
