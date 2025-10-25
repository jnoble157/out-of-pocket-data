"""
Query cache manager for embedding-based similarity search.
Handles caching of high-confidence query results and similarity matching.
"""
import logging
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from embeddings import EmbeddingProvider, EmbeddingConfig
from database import supabase_manager

logger = logging.getLogger(__name__)


@dataclass
class CachedResult:
    """Result from cache lookup."""
    hcpcs_codes: List[str]
    rc_codes: List[str]
    reasoning: str
    confidence_score: float
    original_query: str
    similarity_score: float


class QueryCacheManager:
    """Manages query caching with embedding-based similarity search."""
    
    def __init__(self, embedding_provider: Optional[EmbeddingProvider] = None, cache_config: Optional[EmbeddingConfig] = None):
        """
        Initialize query cache manager.
        
        Args:
            embedding_provider: Embedding provider instance
            cache_config: Embedding configuration
        """
        self.config = cache_config or EmbeddingConfig()
        self.supabase = supabase_manager
        
        # Try to initialize embedding provider, but don't fail if it's not available
        try:
            self.embedding_provider = embedding_provider or EmbeddingProvider(self.config)
            # Test the connection to ensure it actually works
            if self.embedding_provider.test_connection():
                self.embedding_available = True
                logger.info(f"Query cache manager initialized with threshold: {self.config.similarity_threshold}")
            else:
                self.embedding_available = False
                logger.warning("Embedding provider connection test failed")
        except Exception as e:
            logger.warning(f"Embedding provider not available: {e}")
            self.embedding_provider = None
            self.embedding_available = False
            logger.info("Query cache manager initialized without embeddings (cache disabled)")
    
    def check_cache(self, query: str, threshold: Optional[float] = None) -> Optional[CachedResult]:
        """
        Check cache for similar queries and return cached result if found.
        
        Args:
            query: User query to check
            threshold: Similarity threshold (uses config default if None)
            
        Returns:
            CachedResult if similar query found, None otherwise
        """
        if not self.config.cache_enabled or not self.embedding_available:
            logger.debug("Cache is disabled or embeddings not available, skipping cache check")
            return None
        
        try:
            # Generate embedding for the query
            logger.debug(f"Generating embedding for query: {query[:100]}...")
            query_embedding = self.embedding_provider.embed_text(query)
            
            # Search for similar queries
            similarity_threshold = threshold or self.config.similarity_threshold
            logger.debug(f"Searching for similar queries with threshold: {similarity_threshold}")
            
            similar_queries = self.supabase.search_similar_queries(
                embedding=query_embedding,
                threshold=similarity_threshold,
                limit=1
            )
            
            if not similar_queries:
                logger.debug("No similar queries found in cache")
                return None
            
            # Get the best match
            best_match = similar_queries[0]
            similarity_score = best_match.get('similarity', 0.0)
            
            logger.info(f"Cache hit found with similarity: {similarity_score:.3f}")
            
            # Create cached result
            cached_result = CachedResult(
                hcpcs_codes=best_match.get('hcpcs_codes', []),
                rc_codes=best_match.get('rc_codes', []),
                reasoning=best_match.get('reasoning', ''),
                confidence_score=float(best_match.get('confidence_score', 0.0)),
                original_query=best_match.get('user_query', ''),
                similarity_score=similarity_score
            )
            
            return cached_result
            
        except Exception as e:
            logger.error(f"Error checking cache: {e}")
            return None
    
    def store_cache(self, query: str, hcpcs_codes: List[str], rc_codes: List[str], 
                   reasoning: str, confidence: float) -> bool:
        """
        Store query result in cache if confidence is high enough.
        
        Args:
            query: User query
            hcpcs_codes: HCPCS codes from Claude
            rc_codes: RC codes from Claude
            reasoning: Reasoning from Claude
            confidence: Confidence score from Claude
            
        Returns:
            True if stored successfully, False otherwise
        """
        if not self.config.cache_enabled or not self.embedding_available:
            logger.debug("Cache is disabled or embeddings not available, skipping cache storage")
            return False
        
        if confidence < self.config.min_confidence_to_cache:
            logger.debug(f"Confidence {confidence:.3f} below threshold {self.config.min_confidence_to_cache}, not caching")
            return False
        
        try:
            # Generate embedding for the query
            logger.debug(f"Generating embedding for caching query: {query[:100]}...")
            query_embedding = self.embedding_provider.embed_text(query)
            
            # Prepare cache data
            cache_data = {
                'user_query': query,
                'hcpcs_codes': hcpcs_codes,
                'rc_codes': rc_codes,
                'reasoning': reasoning,
                'confidence_score': confidence,
                'query_embedding': query_embedding
            }
            
            # Store in database
            logger.debug(f"Storing query in cache with confidence: {confidence:.3f}")
            result = self.supabase.insert_cached_query(cache_data)
            
            if result:
                logger.info(f"Successfully cached query with {len(hcpcs_codes)} HCPCS and {len(rc_codes)} RC codes")
                return True
            else:
                logger.warning("Failed to store query in cache")
                return False
                
        except Exception as e:
            logger.error(f"Error storing in cache: {e}")
            return False
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        try:
            # Get total cached queries
            result = self.supabase.client.table('query_cache').select('id', count='exact').execute()
            total_queries = result.count if hasattr(result, 'count') else 0
            
            # Get high-confidence queries
            high_conf_result = self.supabase.client.table('query_cache').select(
                'id', count='exact'
            ).gte('confidence_score', 0.90).execute()
            high_conf_queries = high_conf_result.count if hasattr(high_conf_result, 'count') else 0
            
            return {
                'total_queries': total_queries,
                'high_confidence_queries': high_conf_queries,
                'cache_enabled': self.config.cache_enabled,
                'embeddings_available': self.embedding_available,
                'similarity_threshold': self.config.similarity_threshold,
                'min_confidence_to_cache': self.config.min_confidence_to_cache
            }
            
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {
                'total_queries': 0,
                'high_confidence_queries': 0,
                'cache_enabled': self.config.cache_enabled,
                'embeddings_available': self.embedding_available,
                'similarity_threshold': self.config.similarity_threshold,
                'min_confidence_to_cache': self.config.min_confidence_to_cache,
                'error': str(e)
            }
    
    def clear_cache(self) -> bool:
        """
        Clear all cached queries.
        
        Returns:
            True if cleared successfully, False otherwise
        """
        try:
            self.supabase.client.table('query_cache').delete().neq('id', 0).execute()
            logger.info("Cache cleared successfully")
            return True
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
            return False


# Example usage and testing
if __name__ == "__main__":
    # Test the cache manager
    try:
        config = EmbeddingConfig()
        cache_manager = QueryCacheManager(config=config)
        
        # Test cache check
        test_query = "where can I get knee surgery"
        cached_result = cache_manager.check_cache(test_query)
        
        if cached_result:
            print(f"Cache hit! Similarity: {cached_result.similarity_score:.3f}")
            print(f"HCPCS codes: {cached_result.hcpcs_codes}")
            print(f"RC codes: {cached_result.rc_codes}")
        else:
            print("No cache hit found")
        
        # Test cache storage
        test_codes_data = {
            'hcpcs_codes': ['27447', '27448'],
            'rc_codes': ['360'],
            'reasoning': 'Knee surgery procedures',
            'confidence': 0.95
        }
        
        stored = cache_manager.store_cache(
            query=test_query,
            hcpcs_codes=test_codes_data['hcpcs_codes'],
            rc_codes=test_codes_data['rc_codes'],
            reasoning=test_codes_data['reasoning'],
            confidence=test_codes_data['confidence']
        )
        
        print(f"Cache storage: {'Success' if stored else 'Failed'}")
        
        # Get cache stats
        stats = cache_manager.get_cache_stats()
        print(f"Cache stats: {stats}")
        
    except Exception as e:
        print(f"Error testing cache manager: {e}")
