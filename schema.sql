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

-- Grant necessary permissions (adjust as needed for your setup)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON hospitals TO your_app_user;
-- GRANT SELECT, INSERT, UPDATE, DELETE ON medical_operations TO your_app_user;
-- GRANT USAGE, SELECT ON SEQUENCE medical_operations_id_seq TO your_app_user;
