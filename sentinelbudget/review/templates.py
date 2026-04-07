from __future__ import annotations

from sentinelbudget.review.models import ReviewFinding


def render_title_body(finding: ReviewFinding) -> tuple[str, str]:
    if finding.kind == "anomaly_event":
        title = "Unusual spending activity detected"
        body = (
            "An anomaly was detected in recent transactions. "
            f"Evidence: {finding.summary}"
        )
        return title, body

    if finding.kind == "category_spend_pressure":
        category = finding.evidence.get("category_name", "a category")
        title = f"High spend pressure in {category}"
        body = (
            f"Recent spending in {category} is a large share of expenses. "
            f"Evidence: {finding.summary}"
        )
        return title, body

    if finding.kind == "recurring_overdue":
        label = finding.evidence.get("label", "a recurring transaction")
        title = "Expected recurring transaction timing changed"
        body = (
            f"{label} appears overdue relative to prior cadence. "
            f"Evidence: {finding.summary}"
        )
        return title, body

    if finding.kind == "recurring_income_miss":
        title = "Expected recurring income may be delayed"
        body = (
            "A recurring income-like pattern appears overdue in recent data. "
            f"Evidence: {finding.summary}"
        )
        return title, body

    if finding.kind == "goal_drift":
        title = "Goal progress may be drifting"
        body = (
            "Current cashflow trend may reduce progress toward at least one goal. "
            f"Evidence: {finding.summary}"
        )
        return title, body

    if finding.kind == "mtd_cashflow_change":
        title = "Month-to-date cashflow needs attention"
        body = (
            "Month-to-date expenses exceed income in the selected period. "
            f"Evidence: {finding.summary}"
        )
        return title, body

    return (
        "New financial review finding",
        f"A grounded review finding was generated. Evidence: {finding.summary}",
    )
