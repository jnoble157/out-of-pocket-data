-- Medical Pricing Database Schema
-- This schema is designed for PostgreSQL and Supabase compatibility

-- Create hospitals table
CREATE TABLE IF NOT EXISTS hospitals (
    facility_id VARCHAR(255) PRIMARY KEY,
    facility_name VARCHAR(500) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(2) NOT NULL,
    address TEXT NOT NULL,
    source_url TEXT NOT NULL,
    file_version VARCHAR(50) NOT NULL,
    last_updated VARCHAR(50) NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create medical_operations table
CREATE TABLE IF NOT EXISTS medical_operations (
    id SERIAL PRIMARY KEY,
    facility_id VARCHAR(255) NOT NULL REFERENCES hospitals(facility_id) ON DELETE CASCADE,
    codes JSONB NOT NULL,
    rc_code VARCHAR(50) DEFAULT NULL,
    hcpcs_code VARCHAR(50) DEFAULT NULL,
    description TEXT NOT NULL,
    cash_price DECIMAL(10,2) NOT NULL CHECK (cash_price >= 0),
    gross_charge DECIMAL(10,2) NOT NULL CHECK (gross_charge >= 0),
    negotiated_min DECIMAL(10,2) CHECK (negotiated_min >= 0),
    negotiated_max DECIMAL(10,2) CHECK (negotiated_max >= 0),
    currency VARCHAR(3) DEFAULT 'USD',
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CONSTRAINT valid_negotiated_range CHECK (
        negotiated_min IS NULL OR negotiated_max IS NULL OR negotiated_min <= negotiated_max
    )
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_hospitals_state ON hospitals(state);
CREATE INDEX IF NOT EXISTS idx_hospitals_city ON hospitals(city);
CREATE INDEX IF NOT EXISTS idx_medical_operations_facility_id ON medical_operations(facility_id);
CREATE INDEX IF NOT EXISTS idx_medical_operations_codes ON medical_operations USING GIN (codes);
CREATE INDEX IF NOT EXISTS idx_medical_operations_rc_code ON medical_operations(rc_code);
CREATE INDEX IF NOT EXISTS idx_medical_operations_hcpcs_code ON medical_operations(hcpcs_code);
CREATE INDEX IF NOT EXISTS idx_medical_operations_description ON medical_operations(description);
CREATE INDEX IF NOT EXISTS idx_medical_operations_cash_price ON medical_operations(cash_price);

-- Create a composite index for common queries
CREATE INDEX IF NOT EXISTS idx_medical_operations_facility_price ON medical_operations(facility_id, cash_price);

-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers to automatically update updated_at
CREATE TRIGGER update_hospitals_updated_at 
    BEFORE UPDATE ON hospitals 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_medical_operations_updated_at 
    BEFORE UPDATE ON medical_operations 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create a view for common queries
CREATE OR REPLACE VIEW hospital_pricing_summary AS
SELECT 
    h.facility_id,
    h.facility_name,
    h.city,
    h.state,
    COUNT(mo.id) as total_operations,
    AVG(mo.cash_price) as avg_cash_price,
    MIN(mo.cash_price) as min_cash_price,
    MAX(mo.cash_price) as max_cash_price,
    AVG(mo.gross_charge) as avg_gross_charge,
    h.last_updated,
    h.ingested_at
FROM hospitals h
LEFT JOIN medical_operations mo ON h.facility_id = mo.facility_id
GROUP BY h.facility_id, h.facility_name, h.city, h.state, h.last_updated, h.ingested_at;

-- Enable pgvector extension for similarity search
CREATE EXTENSION IF NOT EXISTS vector;

-- Create query_cache table for embedding-based caching
CREATE TABLE IF NOT EXISTS query_cache (
    id SERIAL PRIMARY KEY,
    user_query TEXT NOT NULL,
    hcpcs_codes JSONB NOT NULL DEFAULT '[]',
    rc_codes JSONB NOT NULL DEFAULT '[]',
    reasoning TEXT NOT NULL DEFAULT '',
    confidence_score DECIMAL(3,2) NOT NULL CHECK (confidence_score >= 0.0 AND confidence_score <= 1.0),
    query_embedding vector(1536) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create pgvector index for fast similarity search
CREATE INDEX IF NOT EXISTS idx_query_cache_embedding 
ON query_cache USING ivfflat (query_embedding vector_cosine_ops) 
WITH (lists = 100);

-- Create index on confidence_score for filtering high-confidence results
CREATE INDEX IF NOT EXISTS idx_query_cache_confidence 
ON query_cache(confidence_score);

-- Create trigger to update updated_at timestamp
CREATE TRIGGER update_query_cache_updated_at 
    BEFORE UPDATE ON query_cache 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Create RPC function for similarity search
CREATE OR REPLACE FUNCTION match_query_cache(
    query_embedding vector(1536),
    match_threshold float DEFAULT 0.90,
    match_count int DEFAULT 5
)
RETURNS TABLE (
    id int,
    user_query text,
    hcpcs_codes jsonb,
    rc_codes jsonb,
    reasoning text,
    confidence_score decimal(3,2),
    created_at timestamp with time zone,
    similarity float
)
LANGUAGE sql
AS $$
    SELECT 
        qc.id,
        qc.user_query,
        qc.hcpcs_codes,
        qc.rc_codes,
        qc.reasoning,
        qc.confidence_score,
        qc.created_at,
        1 - (qc.query_embedding <=> query_embedding) as similarity
    FROM query_cache qc
    WHERE qc.confidence_score >= 0.90
    AND 1 - (qc.query_embedding <=> query_embedding) > match_threshold
    ORDER BY qc.query_embedding <=> query_embedding
    LIMIT match_count;
$$;

-- Grant necessary permissions (adjust as needed for your setup)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON hospitals TO your_app_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON medical_operations TO your_app_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON query_cache TO your_app_user;
-- GRANT USAGE, SELECT ON SEQUENCE medical_operations_id_seq TO your_app_user;
-- GRANT USAGE, SELECT ON SEQUENCE query_cache_id_seq TO your_app_user;
