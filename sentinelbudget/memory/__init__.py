"""Semantic memory and pgvector retrieval utilities for SentinelBudget."""

from sentinelbudget.memory.context import assemble_context_bundle
from sentinelbudget.memory.embeddings import (
    DummyDeterministicEmbeddingProvider,
    OllamaEmbeddingProvider,
)
from sentinelbudget.memory.models import (
    ContextBundle,
    MemorySyncSummary,
    SemanticMemoryItem,
    SemanticMemoryMatch,
)
from sentinelbudget.memory.service import SemanticMemoryService

__all__ = [
    "ContextBundle",
    "DummyDeterministicEmbeddingProvider",
    "MemorySyncSummary",
    "OllamaEmbeddingProvider",
    "SemanticMemoryItem",
    "SemanticMemoryMatch",
    "SemanticMemoryService",
    "assemble_context_bundle",
]
