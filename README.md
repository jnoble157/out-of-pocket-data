# Medical Pricing Data Processor

Process hospital pricing data from CSV/JSON files or URLs. Output to database, JSON, or CSV with streaming and automatic format detection.

## Quick Start

```bash
# Install
./install.sh

# Process from local file or URL
python -m src.cli process-file data/hospital.csv --output-format json --output-dir ./output
python -m src.cli process-file "https://example.com/data.csv" --output-format json --output-dir ./output

# Process to database (requires .env setup)
python -m src.cli process-file data/hospital.csv --output-format database
```

## Configuration

Create `.env` file (only for database output):
```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-api-key
```

## Usage

### Basic Commands

```bash
# Process single file
python -m src.cli process-file <file_or_url> --output-format <json|csv|database> --output-dir ./output

# Process directory
python -m src.cli process-directory data/ --output-format json --output-dir ./output

# Query database
python -m src.cli query-hospitals --state TX --limit 10
python -m src.cli query-operations --description "MRI" --min-price 500
```

### Common Options

```bash
--output-format <database|json|csv>   # Output format (default: database)
--output-dir <path>                   # Output directory for json/csv
--include-inpatient                   # Include inpatient records (default: outpatient only)
--allow-missing-price                 # Allow records without cash price
--batch-size <n>                      # Batch size (default: 1000)
--max-workers <n>                     # Worker threads (default: 4)
--max-download-size <mb>              # Download limit in MB (default: 5000)
```

### Examples

```bash
# Process from URL with all records
python -m src.cli process-file "https://example.com/data.csv" \
  --output-format json \
  --include-inpatient \
  --allow-missing-price

# Large file optimization
python -m src.cli process-file large.csv \
  --batch-size 5000 \
  --max-workers 12 \
  --output-format csv

# Process directory of JSON files
python -m src.cli process-directory data/ \
  --pattern "*.json" \
  --output-format csv \
  --output-dir ./results
```

## Features

- **URL download**: Process files from HTTPS URLs (auto-download, up to 5GB)
- **Multiple outputs**: Database (Supabase), JSON, or CSV
- **Smart parsing**: Auto-detects CSV/JSON/NDJSON, handles varying hospital formats
- **Streaming**: Process large files (GB+) without memory issues
- **Filtering**: Optional outpatient-only and cash-price filters
- **Validation**: Standardized medical codes only (CPT, HCPCS, ICD-10, RC)
- **Deduplication**: Removes duplicate records intelligently

## Output Format

**JSON/CSV Output:**
- `{facility_id}_hospitals.json/csv` - Hospital metadata
- `{facility_id}_operations.json/csv` - Medical operations

**Database Output:**
- `hospitals` table: facility info, location, metadata
- `medical_operations` table: codes, prices, descriptions

## Troubleshooting

**Slow processing:** Increase `--batch-size 5000 --max-workers 12`
**Memory issues:** Reduce `--batch-size 500 --max-workers 2`
**Download timeout:** Increase `--max-download-size` or use local file
**Database failed:** Use `--output-format json` to process without database
