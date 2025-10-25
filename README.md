# Medical Pricing Data Processor

Process hospital pricing data from CSV/JSON files into PostgreSQL. Handles large files efficiently with streaming and automatic format detection.

## Quick Start

1. **Install:**
   ```bash
   ./install.sh
   ```

2. **Setup database:**
   ```bash
   # Edit .env with your database URL
   python main.py init-db
   ```

3. **Process data:**
   ```bash
   python main.py process-file data/hospital_data.csv
   ```

## Configuration

Create `.env` file:
```env
DATABASE_URL=postgresql://user:pass@localhost:5432/medical_pricing
LOG_LEVEL=INFO
BATCH_SIZE=1000
```

## Usage

### Process Files
```bash
# Single file
python main.py process-file data/hospital.csv

# Directory of files
python main.py process-directory data/ --pattern "*.csv"

# With custom settings
python main.py process-file data/large_file.csv --batch-size 2000 --max-workers 8
```

### Query Data
```bash
# List hospitals
python main.py query-hospitals --state TX --limit 5

# Search operations
python main.py query-operations --description "MRI" --min-price 500 --max-price 2000

# Test connection
python main.py test-connection
```

## Features

- **Auto-format detection**: CSV, JSON, NDJSON
- **Smart column mapping**: Handles varying hospital formats
- **Streaming processing**: Large files without memory issues
- **Data validation**: Only standardized medical codes (CPT, HCPCS, ICD-10, RC)
- **Deduplication**: Removes duplicate records
- **Batch processing**: Configurable for performance

## Database Schema

**Hospitals:**
- `facility_id`, `facility_name`, `city`, `state`, `address`
- `source_url`, `file_version`, `last_updated`

**Medical Operations:**
- `facility_id`, `codes` (JSONB), `description`
- `cash_price`, `gross_charge`, `negotiated_min/max`
- `currency`, `ingested_at`

## Troubleshooting

**Database connection failed:**
```bash
# Start PostgreSQL
brew services start postgresql  # macOS
sudo systemctl start postgresql  # Linux
```

**Large files:**
```bash
# Increase batch size and workers
python main.py process-file large_file.csv --batch-size 5000 --max-workers 12
```

**Memory issues:**
```bash
# Reduce batch size
python main.py process-file large_file.csv --batch-size 500 --max-workers 2
```