from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class EmbeddingProvider(Protocol):
    @property
    def dimension(self) -> int:
        raise NotImplementedError

    def embed_text(self, text: str) -> list[float]:
        raise NotImplementedError


def validate_embedding_dimension(embedding: list[float], expected_dimension: int) -> None:
    if expected_dimension < 1:
        raise ValueError("Embedding dimension must be positive")

    if len(embedding) != expected_dimension:
        raise ValueError(
            f"Embedding dimension mismatch: expected {expected_dimension}, got {len(embedding)}"
        )

    for value in embedding:
        if not math.isfinite(value):
            raise ValueError("Embedding contains non-finite values")


@dataclass(frozen=True, slots=True)
class DummyDeterministicEmbeddingProvider:
    dimension: int
    salt: str = "sentinelbudget-memory"

    def __post_init__(self) -> None:
        if self.dimension < 1:
            raise ValueError("dimension must be positive")

    def embed_text(self, text: str) -> list[float]:
        if text.strip() == "":
            raise ValueError("text cannot be empty")

        values: list[float] = []
        counter = 0

        while len(values) < self.dimension:
            payload = f"{self.salt}|{text.strip()}|{counter}".encode()
            digest = hashlib.sha256(payload).digest()
            for index in range(0, len(digest), 8):
                chunk = digest[index : index + 8]
                if len(chunk) < 8:
                    continue
                integer = int.from_bytes(chunk, byteorder="big", signed=False)
                values.append((integer / (2**63 - 1)) * 2.0 - 1.0)
                if len(values) == self.dimension:
                    break
            counter += 1

        norm = math.sqrt(sum(value * value for value in values))
        if norm == 0:
            embedding = values
        else:
            embedding = [value / norm for value in values]

        validate_embedding_dimension(embedding, self.dimension)
        return embedding


@dataclass(frozen=True, slots=True)
class OllamaEmbeddingProvider:
    base_url: str
    model: str
    dimension: int
    timeout_seconds: int = 30

    def __post_init__(self) -> None:
        if self.dimension < 1:
            raise ValueError("dimension must be positive")
        if self.timeout_seconds < 1:
            raise ValueError("timeout_seconds must be positive")
        if self.model.strip() == "":
            raise ValueError("model cannot be empty")

    def embed_text(self, text: str) -> list[float]:
        if text.strip() == "":
            raise ValueError("text cannot be empty")

        payload = json.dumps({"model": self.model, "prompt": text}).encode("utf-8")
        url = self.base_url.rstrip("/") + "/api/embeddings"
        request = Request(
            url=url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read().decode("utf-8")
        except HTTPError as exc:
            raise RuntimeError(f"Ollama embedding request failed: {exc.code} {exc.reason}") from exc
        except URLError as exc:
            raise RuntimeError(f"Ollama embedding request failed: {exc.reason}") from exc

        try:
            data = json.loads(body)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Ollama embedding response was not valid JSON") from exc

        embedding_obj = data.get("embedding")
        if not isinstance(embedding_obj, list):
            raise RuntimeError("Ollama embedding response missing 'embedding' array")

        try:
            embedding = [float(value) for value in embedding_obj]
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Ollama embedding response contains non-numeric values") from exc

        validate_embedding_dimension(embedding, self.dimension)
        return embedding
