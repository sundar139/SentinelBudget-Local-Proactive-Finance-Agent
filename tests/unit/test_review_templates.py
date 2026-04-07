from __future__ import annotations

from sentinelbudget.review.models import ReviewFinding
from sentinelbudget.review.templates import render_title_body


def test_templates_are_deterministic_for_same_finding() -> None:
    finding = ReviewFinding(
        kind="category_spend_pressure",
        severity="medium",
        summary="Dining spend increased relative to total expenses.",
        evidence={"category_name": "Dining"},
    )

    first = render_title_body(finding)
    second = render_title_body(finding)

    assert first == second
    assert "Dining" in first[0]


def test_template_fallback_for_unknown_kind() -> None:
    finding = ReviewFinding(
        kind="unknown_kind",
        severity="low",
        summary="Unknown finding summary",
        evidence={},
    )

    title, body = render_title_body(finding)
    assert "New financial review finding" == title
    assert "Unknown finding summary" in body
