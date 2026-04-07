from __future__ import annotations

import pytest
from sentinelbudget.memory.embeddings import (
    DummyDeterministicEmbeddingProvider,
    validate_embedding_dimension,
)


def test_dummy_embedding_provider_is_deterministic() -> None:
    provider = DummyDeterministicEmbeddingProvider(dimension=8)

    first = provider.embed_text("Emergency fund goal")
    second = provider.embed_text("Emergency fund goal")
    third = provider.embed_text("Vacation goal")

    assert first == second
    assert first != third


def test_embedding_dimension_validation_rejects_mismatch() -> None:
    with pytest.raises(ValueError, match="dimension mismatch"):
        validate_embedding_dimension([0.1, 0.2], expected_dimension=3)


def test_embedding_dimension_validation_rejects_non_finite_values() -> None:
    with pytest.raises(ValueError, match="non-finite"):
        validate_embedding_dimension([0.1, float("nan")], expected_dimension=2)


def test_dummy_embedding_provider_rejects_invalid_dimension() -> None:
    with pytest.raises(ValueError, match="positive"):
        DummyDeterministicEmbeddingProvider(dimension=0)
