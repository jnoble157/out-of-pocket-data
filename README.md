# Medical Pricing Data Processor

Process hospital machine‑readable pricing files into clean JSON/CSV or directly into your database. Built for multi‑GB CSV/JSON using streaming, auto‑format detection, normalization, validation, filtering, and de‑duplication.

## Quick start (commands to run)

```bash
# 1) Install
./install.sh

# 2) Process from local file or HTTPS URL (writes JSON to ./output)
python -m src.cli process-file path/to/hospital.csv --output-format json --output-dir ./output
python -m src.cli process-file "https://example.com/data.csv" --output-format json --output-dir ./output

# 3) Process to database (requires Supabase env)
python -m src.cli process-file path/to/hospital.csv --output-format database

# 4) Query data (when using database output)
python -m src.cli query-hospitals --state TX --limit 10
python -m src.cli query-operations --description "MRI" --min-price 500
```

## Tech stack & architecture (simple)

- **Python**: 3.8+
- **Core libs**: `pydantic`, `click`, `requests`, `ijson`, `sqlalchemy`
- **Database**: PostgreSQL/Supabase (via `supabase` Python client)
- **CLI**: `src/cli.py` orchestrates downloading, detection, processing, and writing
- **Processing pipeline**:
  - `src/downloader.py` → secure HTTPS download with size caps
  - `src/format_detector.py` → detect `csv|json|ndjson`
  - `src/streaming_utils.py` → stream rows/items (ijson, csv)
  - `src/csv_processor.py` / `src/json_processor.py` → normalize, filter, dedupe
  - `src/output_writer.py` → writers: `DatabaseWriter|JSONWriter|CSVWriter`

Architecture sketch:

```
file/URL → downloader → format_detector → {csv_processor|json_processor} → output_writer → {Supabase|JSON|CSV}
```

## How to reproduce the demo (env vars, API keys, sample .env)

Database mode uses Supabase credentials. Create `.supabase.env` in the project root:

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-service-or-anon-key
```

Initialize DB objects in your Postgres/Supabase project:

```bash
python -m src.cli init-db
```

Then ingest data to the database:

```bash
python -m src.cli process-file path/to/hospital.csv --output-format database
```

Notes:
- Local Postgres can also be used by setting `DATABASE_URL` (see `env.example`).
- URL inputs must be HTTPS; max download size defaults to 5GB (`--max-download-size`).

## Datasets / synthetic data used + provenance

- Inputs are the public hospital “Price Transparency” machine‑readable files (CSV or JSON) hosted by each hospital system. You provide either a local file path or the official HTTPS URL from the hospital site.
- We do not redistribute datasets; the tool streams and normalizes what you point it to.

## Known limitations & next steps

- Metadata extraction depends on each hospital’s schema; some files may require passing metadata explicitly or fixing source headers.
- Only standardized codes are kept (HCPCS/CPT, RC, ICD‑10 variants); hospital‑specific codes are dropped by design.
- Supabase schema/pgvector are created by `schema.sql`, but permissions/rpc enablement may vary by project; verify after `init-db`.
- Next steps: broader schema mappers per health system, richer quality checks, and web UI for comparisons.

## Usage

### Common options

```bash
--output-format <database|json|csv>
--output-dir <path>                   # for json/csv outputs
--include-inpatient                   # include inpatient (default: outpatient only)
--allow-missing-price                 # allow rows without cash price
--batch-size <n>                      # default: 1000
--max-workers <n>                     # default: 4
--max-download-size <mb>              # default: 5000
```

### Examples

```bash
# URL with all records
python -m src.cli process-file "https://example.com/data.csv" --output-format json --include-inpatient --allow-missing-price

# Large file tuning
python -m src.cli process-file large.csv --batch-size 5000 --max-workers 12 --output-format csv

# Directory of JSON files
python -m src.cli process-directory data/ --pattern "*.json" --output-format csv --output-dir ./results
```

## Output format

JSON/CSV output files:
- `{facility_id}_hospitals.json|csv`
- `{facility_id}_operations.json|csv`

Database tables (Postgres/Supabase):
- `hospitals`
- `medical_operations`

## Troubleshooting

- Slow processing: increase `--batch-size 5000 --max-workers 12`
- Memory issues: reduce `--batch-size 500 --max-workers 2`
- Download timeout: increase `--max-download-size` or use a local file
- Database issues: switch to `--output-format json` to validate parsing first


