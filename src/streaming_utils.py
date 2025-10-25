"""
Advanced streaming utilities for medical pricing data processing.
Based on patterns from temp/pipelines for efficient large file processing.
"""
import json
import csv
import logging
from pathlib import Path
from typing import Iterator, Dict, Any, Optional, List
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re

logger = logging.getLogger(__name__)


def stream_json_array(file_path: Path, array_key: str = "standard_charge_information") -> Iterator[Dict[str, Any]]:
    """
    Stream JSON array from large JSON files without loading entire file into memory.
    Uses ijson for efficient streaming parsing.
    
    Args:
        file_path: Path to JSON file
        array_key: Key containing the array to stream (or empty for direct arrays)
        
    Yields:
        Individual items from the array
    """
    items_yielded = 0
    try:
        import ijson
        
        logger.info(f"Starting to stream JSON array from {file_path} using ijson")
        
        # Check if file is a direct array (starts with '[')
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            first_line = f.readline().strip()
        
        if first_line.startswith('['):
            # Direct array - use root path
            array_path = "item"
            logger.info("File is a direct JSON array")
        else:
            # Wrapped array - use the specified key
            array_path = f"{array_key}.item"
            logger.info(f"File has wrapped array with key: {array_key}")
        
        with open(file_path, 'rb') as f:
            parser = ijson.items(f, array_path)
            for item in parser:
                items_yielded += 1
                if items_yielded % 1000 == 0:
                    logger.info(f"Yielded {items_yielded} JSON items so far")
                yield item
                        
        logger.info(f"Finished streaming JSON array. Total items yielded: {items_yielded}")
                        
    except ImportError:
        logger.error("ijson library not available. Please install with: pip install ijson")
        raise
    except Exception as e:
        logger.error(f"Error streaming JSON array from {file_path}: {e}")
        raise


def stream_json_metadata(file_path: Path) -> Dict[str, Any]:
    """
    Extract metadata from JSON file without loading entire file.
    
    Args:
        file_path: Path to JSON file
        
    Returns:
        Dictionary with metadata fields
    """
    metadata = {}
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            # Read first few lines to get metadata
            for i, line in enumerate(f):
                if i > 50:  # Limit to first 50 lines
                    break
                
                # Look for common metadata fields
                if '"hospital_name"' in line:
                    try:
                        # Extract hospital name
                        match = re.search(r'"hospital_name":\s*"([^"]+)"', line)
                        if match:
                            metadata['hospital_name'] = match.group(1)
                    except:
                        pass
                
                if '"hospital_address"' in line:
                    try:
                        # Extract hospital address
                        match = re.search(r'"hospital_address":\s*\["([^"]+)"', line)
                        if match:
                            metadata['hospital_address'] = [match.group(1)]
                    except:
                        pass
                
                if '"version"' in line:
                    try:
                        match = re.search(r'"version":\s*"([^"]+)"', line)
                        if match:
                            metadata['version'] = match.group(1)
                    except:
                        pass
                
                if '"last_updated_on"' in line:
                    try:
                        match = re.search(r'"last_updated_on":\s*"([^"]+)"', line)
                        if match:
                            metadata['last_updated_on'] = match.group(1)
                    except:
                        pass
                
                # Stop if we've found the main data section
                if '"standard_charge_information"' in line:
                    break
                    
    except Exception as e:
        logger.warning(f"Could not extract metadata from {file_path}: {e}")
    
    return metadata


def stream_csv_rows(file_path: Path, skip_metadata_rows: int = 0) -> Iterator[Dict[str, str]]:
    """
    Stream CSV rows without loading entire file into memory.
    
    Args:
        file_path: Path to CSV file
        skip_metadata_rows: Number of metadata rows to skip
        
    Yields:
        Dictionary rows from CSV
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            
            # Skip metadata rows
            for _ in range(skip_metadata_rows):
                try:
                    next(reader)
                except StopIteration:
                    break
            
            # Get header row (first row after skipping metadata)
            try:
                header = next(reader)
                header = [col.strip() for col in header]
            except StopIteration:
                return
            
            # Process data rows
            for row in reader:
                # Pad row with empty strings if it's shorter than header
                while len(row) < len(header):
                    row.append('')
                
                # Create dictionary mapping header to values
                row_dict = dict(zip(header, row))
                yield row_dict
                
    except Exception as e:
        logger.error(f"Error streaming CSV from {file_path}: {e}")
        raise


def detect_metadata_rows(file_path: Path, max_rows: int = 5) -> int:
    """
    Detect how many metadata rows to skip before header row.
    
    Args:
        file_path: Path to CSV file
        max_rows: Maximum rows to check
        
    Returns:
        Number of rows to skip to get to header
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            rows = []
            
            for i, row in enumerate(reader):
                if i >= max_rows:
                    break
                rows.append(row)
        
        # Heuristics to detect header row
        for i, row in enumerate(rows):
            # Header likely contains these keywords
            header_keywords = [
                'description', 'code', 'charge', 'price', 'procedure',
                'service', 'billing', 'standard_charge', 'setting', 'gross', 'discounted'
            ]
            
            # Check if row has multiple header-like columns
            row_lower = [str(cell).lower() for cell in row]
            keyword_count = sum(
                any(keyword in cell for keyword in header_keywords)
                for cell in row_lower
            )
            
            # Special case: look for the specific pattern in your CSV files
            # Row should have 'description' as first column and 'setting' somewhere
            if (len(row) > 0 and 
                str(row[0]).lower().strip() == 'description' and
                any('setting' in str(cell).lower() for cell in row)):
                return i
            
            # If >30% of columns look like headers, this is probably the header row
            if len(row) > 0 and keyword_count / len(row) > 0.3:
                return i
        
        # Default: assume row 0 is header
        return 0
        
    except Exception as e:
        logger.warning(f"Could not detect metadata rows in {file_path}: {e}")
        return 0


def safe_decimal(value: Any) -> Optional[Decimal]:
    """
    Safely convert value to Decimal.
    
    Args:
        value: Value to convert
        
    Returns:
        Decimal value or None if invalid
    """
    if value is None or value == '':
        return None
    
    # Handle string values
    if isinstance(value, str):
        # Remove currency symbols, commas, whitespace
        cleaned = value.replace('$', '').replace(',', '').strip()
        if not cleaned or cleaned.upper() in ('N/A', 'NULL', 'NONE', '-'):
            return None
        value = cleaned
    
    try:
        result = Decimal(str(value))
        # Return None for zero or negative values
        return result if result > 0 else None
    except (InvalidOperation, ValueError):
        return None


def is_standardized_code_type(code_type: str) -> bool:
    """
    Check if a code type is nationally standardized (not hospital-specific).
    
    Standardized codes:
    - CPT (Current Procedural Terminology)
    - HCPCS (Healthcare Common Procedure Coding System)
    - ICD-10 and all variations (ICD-10-CM, ICD-10-PCS, etc.)
    - RC (Revenue Code)
    
    Hospital-specific codes (excluded):
    - CDM (Charge Description Master)
    - APC (Ambulatory Payment Classification)
    - DRG / MS-DRG (Diagnosis-Related Group)
    - NDC (National Drug Code)
    - internal, unknown, etc.
    
    Args:
        code_type: Code type string
        
    Returns:
        True if standardized, False otherwise
    """
    standardized_types = {
        'CPT',
        'HCPCS',  # Healthcare Common Procedure Coding System
        'ICD-10',
        'ICD-10-CM',
        'ICD-10-PCS',
        'RC'
    }
    return code_type.upper() in standardized_types


def write_ndjson(file_path: Path, records: Iterator[Dict[str, Any]]) -> int:
    """
    Write records to NDJSON file.
    
    Args:
        file_path: Output file path
        records: Iterator of records to write
        
    Returns:
        Number of records written
    """
    count = 0
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            for record in records:
                f.write(json.dumps(record) + '\n')
                count += 1
                
                if count % 1000 == 0:
                    logger.info(f"Written {count} records to {file_path}")
                    
    except Exception as e:
        logger.error(f"Error writing NDJSON to {file_path}: {e}")
        raise
    
    return count


def ensure_directory(path: Path) -> None:
    """
    Ensure directory exists, creating if necessary.
    
    Args:
        path: Directory path
    """
    path.mkdir(parents=True, exist_ok=True)


def log_pipeline_step(logger, step_name: str, **kwargs) -> None:
    """
    Log a pipeline step with structured data.
    
    Args:
        logger: Logger instance
        step_name: Name of the pipeline step
        **kwargs: Additional data to log
    """
    logger.info(f"Pipeline step: {step_name}", extra=kwargs)


def log_file_processing(logger, file_path: Path, operation: str, **kwargs) -> None:
    """
    Log file processing operation.
    
    Args:
        logger: Logger instance
        file_path: Path to file being processed
        operation: Operation being performed
        **kwargs: Additional data to log
    """
    logger.info(f"File processing: {operation} on {file_path}", extra=kwargs)
