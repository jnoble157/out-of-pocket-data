"""
Embedding provider for query similarity search.
"""
from .embedding_provider import EmbeddingProvider
from .config import EmbeddingConfig

__all__ = [
    'EmbeddingProvider',
    'EmbeddingConfig'
]
