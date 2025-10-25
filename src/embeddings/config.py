"""
Configuration for embedding providers.
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.supabase.env')


class EmbeddingConfig:
    """Configuration for embedding providers."""
    
    def __init__(self):
        # Embedding model selection (OpenAI's cheapest model)
        self.model = os.getenv('EMBEDDING_MODEL', 'text-embedding-3-small')
        
        # Similarity threshold for cache hits (0.0 to 1.0)
        self.similarity_threshold = float(os.getenv('SIMILARITY_THRESHOLD', '0.90'))
        
        # Cache enabled/disabled flag
        self.cache_enabled = os.getenv('CACHE_ENABLED', 'true').lower() == 'true'
        
        # Batch size for embeddings
        self.batch_size = int(os.getenv('EMBEDDING_BATCH_SIZE', '10'))
        
        # Minimum confidence score to store in cache
        self.min_confidence_to_cache = float(os.getenv('MIN_CONFIDENCE_TO_CACHE', '0.90'))
        
        # API key for embedding service (OpenAI)
        self.api_key = os.getenv('OPENAI_API_KEY')
        
        # Embedding dimensions (for text-embedding-3-small)
        self.embedding_dimensions = 1536
    
    def validate(self) -> bool:
        """Validate configuration."""
        if self.cache_enabled and not self.api_key:
            raise ValueError("OPENAI_API_KEY is required when cache is enabled")
        
        if not 0.0 <= self.similarity_threshold <= 1.0:
            raise ValueError("Similarity threshold must be between 0.0 and 1.0")
        
        if not 0.0 <= self.min_confidence_to_cache <= 1.0:
            raise ValueError("Min confidence to cache must be between 0.0 and 1.0")
        
        if self.batch_size <= 0:
            raise ValueError("Batch size must be positive")
        
        return True
