"""
Embedding provider for query similarity search using OpenAI.
"""
import logging
from typing import List, Optional
import openai

from .config import EmbeddingConfig

logger = logging.getLogger(__name__)


class EmbeddingProvider:
    """
    OpenAI embedding provider.
    Uses text-embedding-3-small model for cost-effective embeddings.
    """
    
    def __init__(self, config: Optional[EmbeddingConfig] = None):
        """
        Initialize embedding provider.
        
        Args:
            config: Embedding configuration
        """
        self.config = config or EmbeddingConfig()
        self.config.validate()
        
        if not self.config.api_key:
            raise ValueError("OPENAI_API_KEY is required for embeddings")
        
        try:
            self.client = openai.OpenAI(api_key=self.config.api_key)
            logger.info(f"Embedding provider initialized with model: {self.config.model}")
        except Exception as e:
            logger.error(f"Failed to initialize embedding client: {e}")
            raise
    
    def embed_text(self, text: str) -> List[float]:
        """
        Generate embedding for a single text.
        
        Args:
            text: Text to embed
            
        Returns:
            List of embedding values
        """
        try:
            logger.debug(f"Generating embedding for text: {text[:100]}...")
            
            result = self.client.embeddings.create(
                input=text,
                model=self.config.model
            )
            
            if not result.data or len(result.data) == 0:
                raise ValueError("No embeddings returned from API")
            
            embedding = result.data[0].embedding
            logger.debug(f"Generated embedding with {len(embedding)} dimensions")
            
            return embedding
            
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            raise
    
    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for multiple texts.
        
        Args:
            texts: List of texts to embed
            
        Returns:
            List of embedding vectors
        """
        if not texts:
            return []
        
        try:
            logger.debug(f"Generating embeddings for {len(texts)} texts")
            
            all_embeddings = []
            for i in range(0, len(texts), self.config.batch_size):
                batch = texts[i:i + self.config.batch_size]
                
                result = self.client.embeddings.create(
                    input=batch,
                    model=self.config.model
                )
                
                if not result.data:
                    raise ValueError("No embeddings returned from API")
                
                batch_embeddings = [item.embedding for item in result.data]
                all_embeddings.extend(batch_embeddings)
            
            logger.debug(f"Generated {len(all_embeddings)} embeddings")
            return all_embeddings
            
        except Exception as e:
            logger.error(f"Error generating batch embeddings: {e}")
            raise
    
    def test_connection(self) -> bool:
        """
        Test the API connection.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            test_embedding = self.embed_text("test query")
            
            if not test_embedding or len(test_embedding) != self.config.embedding_dimensions:
                logger.error(f"Invalid embedding dimensions: {len(test_embedding)}")
                return False
            
            logger.info("API connection test successful")
            return True
            
        except Exception as e:
            logger.error(f"API connection test failed: {e}")
            return False


# Example usage and testing
if __name__ == "__main__":
    try:
        config = EmbeddingConfig()
        provider = EmbeddingProvider(config)
        
        if provider.test_connection():
            print("API connection successful!")
            
            test_text = "where can I get knee surgery"
            embedding = provider.embed_text(test_text)
            print(f"Single embedding generated with {len(embedding)} dimensions")
            
            test_texts = ["knee surgery", "hip replacement", "cardiac catheterization"]
            embeddings = provider.embed_batch(test_texts)
            print(f"Batch embeddings generated: {len(embeddings)} embeddings")
        else:
            print("API connection failed!")
            
    except Exception as e:
        print(f"Error testing embedding provider: {e}")
