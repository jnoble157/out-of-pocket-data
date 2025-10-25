"""
File format detection for hospital price transparency files.
Based on patterns from temp/pipelines for automatic format detection.
"""
import json
from pathlib import Path
from typing import Literal
import logging

logger = logging.getLogger(__name__)

FileFormat = Literal['json', 'csv', 'ndjson', 'unknown']


def detect_file_format(file_path: Path) -> FileFormat:
    """
    Detect file format from extension and content.
    
    Args:
        file_path: Path to file
        
    Returns:
        Format type: 'json', 'csv', 'ndjson', or 'unknown'
    """
    file_path = Path(file_path)
    
    # Check extension first
    ext = file_path.suffix.lower()
    
    if ext == '.csv':
        return 'csv'
    elif ext in ['.ndjson', '.jsonl']:
        return 'ndjson'
    elif ext == '.json':
        # Could be regular JSON or NDJSON
        return detect_json_type(file_path)
    
    # Try to detect from content
    try:
        with open(file_path, 'rb') as f:
            # Read first 1KB
            sample = f.read(1024).decode('utf-8', errors='ignore')
            
            # Check for CSV
            if ',' in sample and '\n' in sample:
                # Count commas per line
                lines = sample.split('\n')[:3]
                comma_counts = [line.count(',') for line in lines]
                # If consistent comma count, likely CSV
                if len(set(comma_counts)) == 1 and comma_counts[0] > 3:
                    return 'csv'
            
            # Check for JSON
            sample_stripped = sample.strip()
            if sample_stripped.startswith('{') or sample_stripped.startswith('['):
                return detect_json_type(file_path)
    
    except Exception:
        pass
    
    return 'unknown'


def detect_json_type(file_path: Path) -> Literal['json', 'ndjson']:
    """
    Determine if JSON file is regular JSON or NDJSON.
    
    Args:
        file_path: Path to JSON file
        
    Returns:
        'json' or 'ndjson'
    """
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            # Read first few lines
            first_line = f.readline().strip()
            second_line = f.readline().strip()
            
            # NDJSON: each line is valid JSON
            if first_line and second_line:
                try:
                    json.loads(first_line)
                    json.loads(second_line)
                    return 'ndjson'
                except json.JSONDecodeError:
                    pass
            
            # Regular JSON: starts with { or [
            if first_line.startswith('{') or first_line.startswith('['):
                return 'json'
    
    except Exception:
        pass
    
    # Default to regular JSON
    return 'json'


def is_supported_format(file_path: Path) -> bool:
    """
    Check if file format is supported.
    
    Args:
        file_path: Path to file
        
    Returns:
        True if supported, False otherwise
    """
    fmt = detect_file_format(file_path)
    return fmt in ['json', 'csv', 'ndjson']


def get_file_info(file_path: Path) -> dict:
    """
    Get basic file information.
    
    Args:
        file_path: Path to file
        
    Returns:
        Dictionary with file information
    """
    try:
        stat = file_path.stat()
        return {
            'size_bytes': stat.st_size,
            'size_mb': stat.st_size / (1024 * 1024),
            'modified_time': stat.st_mtime,
            'format': detect_file_format(file_path)
        }
    except Exception as e:
        logger.error(f"Could not get file info for {file_path}: {e}")
        return {
            'size_bytes': 0,
            'size_mb': 0,
            'modified_time': 0,
            'format': 'unknown'
        }
