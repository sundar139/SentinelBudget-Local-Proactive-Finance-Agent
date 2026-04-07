from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from uuid import UUID


def _quantized_amount(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def compute_source_row_hash(payload: Mapping[str, str]) -> str:
    canonical_payload = {key: payload[key].strip() for key in sorted(payload)}
    encoded = json.dumps(canonical_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def compute_trans_key(
    account_id: UUID,
    ts: datetime,
    amount: Decimal,
    currency: str,
    trans_type: str,
    description: str | None,
    source_row_hash: str,
) -> int:
    timestamp = ts.astimezone(UTC).isoformat()
    normalized_amount = f"{_quantized_amount(amount):.2f}"
    normalized_description = (description or "").strip().lower()

    digest_input = "|".join(
        [
            str(account_id),
            timestamp,
            normalized_amount,
            currency.upper(),
            trans_type.lower(),
            normalized_description,
            source_row_hash,
        ]
    ).encode("utf-8")

    digest = hashlib.sha256(digest_input).digest()
    value = int.from_bytes(digest[:8], byteorder="big", signed=False) & ((1 << 63) - 1)
    return value if value > 0 else 1


def dedup_fingerprint(
    account_id: UUID,
    ts: datetime,
    amount: Decimal,
    description: str | None,
) -> tuple[str, str, str, str]:
    return (
        str(account_id),
        ts.astimezone(UTC).isoformat(),
        f"{_quantized_amount(amount):.2f}",
        (description or "").strip().lower(),
    )
