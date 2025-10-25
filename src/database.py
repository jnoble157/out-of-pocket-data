"""
Database connection and configuration management.
"""
import os
import logging
from typing import Optional, Dict, Any
from contextlib import contextmanager
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.supabase.env')

logger = logging.getLogger(__name__)


class DatabaseConfig:
    """Database configuration management."""
    
    def __init__(self):
        self.database_url = os.getenv('DATABASE_URL')
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_KEY')
        
        # Set default database URL if not provided
        if not self.database_url:
            self.database_url = "postgresql://localhost:5432/medical_pricing"
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Parse database URL and return connection parameters."""
        # Parse the DATABASE_URL
        # Format: postgresql://username:password@host:port/database
        if self.database_url.startswith('postgresql://'):
            url = self.database_url
        else:
            url = self.database_url
        
        return {
            'dsn': url,
            'cursor_factory': RealDictCursor
        }


class DatabaseManager:
    """Database connection and operation management."""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig()
        self.connection_pool = None
        self.engine = None
        self.session_factory = None
        
    def initialize(self, min_connections: int = 1, max_connections: int = 10):
        """Initialize database connections and engine."""
        try:
            # Create connection pool
            params = self.config.get_connection_params()
            self.connection_pool = SimpleConnectionPool(
                min_connections,
                max_connections,
                dsn=params['dsn'],
                cursor_factory=params['cursor_factory']
            )
            
            # Create SQLAlchemy engine
            self.engine = create_engine(
                self.config.database_url,
                pool_size=max_connections,
                max_overflow=0,
                pool_pre_ping=True
            )
            
            # Create session factory
            self.session_factory = sessionmaker(bind=self.engine)
            
            logger.info("Database connections initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database connections: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Get a database connection from the pool."""
        if not self.connection_pool:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        
        conn = None
        try:
            conn = self.connection_pool.getconn()
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                self.connection_pool.putconn(conn)
    
    @contextmanager
    def get_session(self):
        """Get a SQLAlchemy session."""
        if not self.session_factory:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()
    
    def execute_sql_file(self, file_path: str):
        """Execute SQL commands from a file."""
        with open(file_path, 'r') as f:
            sql_content = f.read()
        
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_content)
                conn.commit()
                logger.info(f"Successfully executed SQL file: {file_path}")
    
    def test_connection(self) -> bool:
        """Test database connection."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    logger.info(f"Database test result: {result}")
                    # Access the value from the RealDictRow
                    return result['?column?'] == 1
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def close(self):
        """Close all database connections."""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Database connections closed")
        
        if self.engine:
            self.engine.dispose()
            logger.info("SQLAlchemy engine disposed")


class SupabaseManager:
    """Supabase-specific database operations."""
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        self.config = config or DatabaseConfig()
        self.client = None
        
        if not self.config.supabase_url or not self.config.supabase_key:
            logger.warning("Supabase credentials not provided. Supabase features will be disabled.")
    
    def initialize(self):
        """Initialize Supabase client."""
        try:
            from supabase import create_client, Client
            
            self.client: Client = create_client(
                self.config.supabase_url,
                self.config.supabase_key
            )
            logger.info("Supabase client initialized successfully")
            
        except ImportError:
            logger.error("Supabase package not installed. Install with: pip install supabase")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {e}")
            raise
    
    def insert_hospital(self, hospital_data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert hospital data into Supabase."""
        if not self.client:
            raise RuntimeError("Supabase client not initialized")
        
        try:
            result = self.client.table('hospitals').insert(hospital_data).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Failed to insert hospital data: {e}")
            raise
    
    def insert_medical_operation(self, operation_data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert medical operation data into Supabase."""
        if not self.client:
            raise RuntimeError("Supabase client not initialized")
        
        try:
            result = self.client.table('medical_operations').insert(operation_data).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"Failed to insert medical operation data: {e}")
            raise
    
    def batch_insert_medical_operations(self, operations_data: list) -> list:
        """Batch insert medical operations data into Supabase."""
        if not self.client:
            raise RuntimeError("Supabase client not initialized")
        
        try:
            result = self.client.table('medical_operations').insert(operations_data).execute()
            return result.data if result.data else []
        except Exception as e:
            logger.error(f"Failed to batch insert medical operations data: {e}")
            raise


# Global database manager instance
db_manager = DatabaseManager()
supabase_manager = SupabaseManager()
