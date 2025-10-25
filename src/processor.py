"""
Advanced data streaming and normalization pipeline for medical pricing data.
Based on patterns from temp/pipelines for efficient large file processing.
"""
import json
import logging
import asyncio
import csv
import re
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from .models import (
    Hospital, DataIngestionResult, generate_facility_id
)
from .database import db_manager
from .streaming_utils import (
    stream_json_metadata, detect_metadata_rows
)
from .format_detector import detect_file_format, is_supported_format, get_file_info
from .csv_processor import CSVProcessor
from .json_processor import JSONProcessor
from .output_writer import OutputWriter

logger = logging.getLogger(__name__)


class DataProcessor:
    """Main data processing class for medical pricing data."""

    def __init__(self, batch_size: int = 1000, max_workers: int = 4, output_writer: Optional[OutputWriter] = None,
                 filter_outpatient_only: bool = True, require_cash_price: bool = True):
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.writer = output_writer
        self.csv_processor = CSVProcessor(
            batch_size=batch_size,
            output_writer=output_writer,
            filter_outpatient_only=filter_outpatient_only,
            require_cash_price=require_cash_price
        )
        self.json_processor = JSONProcessor(
            batch_size=batch_size,
            output_writer=output_writer,
            filter_outpatient_only=filter_outpatient_only,
            require_cash_price=require_cash_price
        )
    
    async def process_file(self, file_path: Union[str, Path], 
                          hospital_metadata: Optional[Dict[str, Any]] = None) -> DataIngestionResult:
        """
        Process a single CSV or JSON file using advanced format detection and streaming.
        
        Args:
            file_path: Path to the data file
            hospital_metadata: Optional hospital metadata to use instead of extracting from file
            
        Returns:
            DataIngestionResult with processing statistics
        """
        file_path = Path(file_path)
        start_time = datetime.now()
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Detect file format
        file_format = detect_file_format(file_path)
        if not is_supported_format(file_path):
            raise ValueError(f"Unsupported file format: {file_format}")
        
        # Get file info
        file_info = get_file_info(file_path)
        size_mb = file_info['size_mb']
        logger.info(f"Processing {file_format} file: {file_path.name} ({size_mb:.1f} MB)")
        
        # Extract hospital metadata if not provided
        if not hospital_metadata:
            hospital_metadata = await self._extract_hospital_metadata(file_path)
        
        # Create or update hospital record
        hospital = await self._create_hospital_record(hospital_metadata)
        
        # Process the data file based on detected format
        if file_format == 'csv':
            result = await self.csv_processor.process_csv_file(file_path, hospital.facility_id)
        elif file_format in ['json', 'ndjson']:
            result = await self.json_processor.process_json_file(file_path, hospital.facility_id)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
        
        processing_time = (datetime.now() - start_time).total_seconds()
        
        return DataIngestionResult(
            facility_id=hospital.facility_id,
            total_records=result['total_records'],
            successful_records=result['successful_records'],
            failed_records=result['failed_records'],
            errors=result['errors'],
            processing_time=processing_time
        )
    
    async def _extract_hospital_metadata(self, file_path: Path) -> Dict[str, Any]:
        """Extract hospital metadata using advanced streaming patterns."""
        # Initialize metadata without defaults - will fail if not extracted
        metadata = {
            'facility_id': None,
            'facility_name': None,
            'city': None,
            'state': None,
            'address': None,
            'source_url': str(file_path.absolute()),
            'file_version': None,
            'last_updated': None
        }
        
        # Try to extract metadata from file content using streaming
        try:
            if file_path.suffix.lower() == '.json':
                # Use streaming JSON metadata extraction
                json_metadata = stream_json_metadata(file_path)
                logger.info(f"JSON metadata extracted: {json_metadata}")
                if json_metadata:
                    # Map JSON metadata fields to database fields
                    mapped_metadata = self._map_json_metadata(json_metadata)
                    metadata.update(mapped_metadata)
                else:
                    # If no metadata found in JSON, try to extract from filename
                    logger.info(f"No JSON metadata found, extracting from filename: {file_path.name}")
                    filename_metadata = self._extract_metadata_from_filename(file_path)
                    if filename_metadata:
                        logger.info(f"Extracted filename metadata: {filename_metadata}")
                        metadata.update(filename_metadata)
                    else:
                        logger.warning(f"Could not extract metadata from filename: {file_path.name}")
            elif file_path.suffix.lower() == '.csv':
                # Try to extract from CSV metadata rows
                metadata_rows = detect_metadata_rows(file_path)
                if metadata_rows > 0:
                    # Read metadata rows
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        reader = csv.reader(f)
                        rows = []
                        for i, row in enumerate(reader):
                            if i >= metadata_rows:
                                break
                            rows.append(row)
                        
                        # Handle different metadata row patterns
                        if len(rows) >= 2:
                            # Pattern 1: First row is headers, second row is values
                            if len(rows[0]) > 0 and 'hospital_name' in str(rows[0][0]).lower():
                                keys = rows[0]
                                values = rows[1]
                                for k, v in zip(keys, values):
                                    if k and v:
                                        key = k.strip().lower()
                                        value = v.strip()
                                        
                                        # Use exact column names from the CSV
                                        if key == 'hospital_name':
                                            metadata['facility_name'] = value
                                        elif key == 'last_updated_on':
                                            metadata['last_updated'] = value
                                        elif key == 'version':
                                            metadata['file_version'] = value
                                        elif key == 'hospital_location':
                                            # Try to extract city and state from location
                                            location_parts = value.split(',')
                                            if len(location_parts) >= 2:
                                                metadata['city'] = location_parts[0].strip()
                                                state_part = location_parts[1].strip()
                                                # Extract state (usually 2 letters)
                                                state_match = re.search(r'\b([A-Z]{2})\b', state_part)
                                                if state_match:
                                                    metadata['state'] = state_match.group(1)
                                        elif key == 'hospital_address':
                                            metadata['address'] = value
                                            # Try to extract city and state from address if not already set
                                            if not metadata['city']:
                                                # Try different parsing approaches
                                                # Approach 1: Split by comma
                                                address_parts = value.split(',')
                                                if len(address_parts) >= 2:
                                                    metadata['city'] = address_parts[0].strip()
                                                    state_part = address_parts[-1].strip()
                                                    state_match = re.search(r'\b([A-Z]{2})\b', state_part)
                                                    if state_match:
                                                        metadata['state'] = state_match.group(1)
                                                else:
                                                    # Approach 2: Look for state pattern in the full address
                                                    state_match = re.search(r'\b([A-Z]{2})\s+\d{5}', value)
                                                    if state_match:
                                                        metadata['state'] = state_match.group(1)
                                                        # Extract city - everything before the state
                                                        city_part = value[:state_match.start()].strip()
                                                        # Get the last word before state (likely city name)
                                                        city_words = city_part.split()
                                                        if city_words:
                                                            metadata['city'] = city_words[-1]
                            # Pattern 2: First row is headers, second row is values (different format)
                            elif len(rows[0]) > 0 and any('hospital' in str(cell).lower() for cell in rows[0]):
                                keys = rows[0]
                                values = rows[1]
                                for k, v in zip(keys, values):
                                    if k and v:
                                        key = k.strip().lower()
                                        value = v.strip()
                                        
                                        # Use exact column names from the CSV
                                        if key == 'hospital_name':
                                            metadata['facility_name'] = value
                                        elif key == 'last_updated_on':
                                            metadata['last_updated'] = value
                                        elif key == 'version':
                                            metadata['file_version'] = value
                                        elif key == 'hospital_location':
                                            # Try to extract city and state from location
                                            location_parts = value.split(',')
                                            if len(location_parts) >= 2:
                                                metadata['city'] = location_parts[0].strip()
                                                state_part = location_parts[1].strip()
                                                # Extract state (usually 2 letters)
                                                state_match = re.search(r'\b([A-Z]{2})\b', state_part)
                                                if state_match:
                                                    metadata['state'] = state_match.group(1)
                                        elif key == 'hospital_address':
                                            metadata['address'] = value
                                            # Try to extract city and state from address if not already set
                                            if not metadata['city']:
                                                # Try different parsing approaches
                                                # Approach 1: Split by comma
                                                address_parts = value.split(',')
                                                if len(address_parts) >= 2:
                                                    metadata['city'] = address_parts[0].strip()
                                                    state_part = address_parts[-1].strip()
                                                    state_match = re.search(r'\b([A-Z]{2})\b', state_part)
                                                    if state_match:
                                                        metadata['state'] = state_match.group(1)
                                                else:
                                                    # Approach 2: Look for state pattern in the full address
                                                    state_match = re.search(r'\b([A-Z]{2})\s+\d{5}', value)
                                                    if state_match:
                                                        metadata['state'] = state_match.group(1)
                                                        # Extract city - everything before the state
                                                        city_part = value[:state_match.start()].strip()
                                                        # Get the last word before state (likely city name)
                                                        city_words = city_part.split()
                                                        if city_words:
                                                            metadata['city'] = city_words[-1]
        except Exception as e:
            logger.warning(f"Could not extract additional metadata from {file_path}: {e}")
        
        # If we didn't get hospital name from file content, extract from filename
        if not metadata['facility_name']:
            # Expected format: number_hospital-name_standardcharges.csv
            filename_parts = file_path.stem.split('_')
            if len(filename_parts) >= 3 and filename_parts[-1] == 'standardcharges':
                facility_number = filename_parts[0]
                hospital_name = '_'.join(filename_parts[1:-1]).replace('-', ' ').title()
            else:
                # Fallback: use file name as hospital name
                hospital_name = file_path.stem.replace('-', ' ').replace('_', ' ').title()
                facility_number = None
            
            metadata['facility_name'] = hospital_name
        
        # Generate facility_id using the hospital name
        facility_id = generate_facility_id(metadata['facility_name'])
        metadata['facility_id'] = facility_id
        
        # Validate that required metadata was extracted - no defaults allowed
        required_fields = ['facility_name', 'city', 'state', 'address']
        missing_fields = [field for field in required_fields if not metadata.get(field)]
        
        if missing_fields:
            raise ValueError(f"Failed to extract required metadata fields: {missing_fields}. "
                           f"File: {file_path.name}. "
                           f"Please ensure the file contains proper metadata or provide it explicitly.")
        
        return metadata
    
    def _extract_metadata_from_filename(self, file_path: Path) -> Dict[str, Any]:
        """Extract metadata from filename when file content doesn't contain metadata."""
        metadata = {}
        filename = file_path.stem  # Get filename without extension
        
        try:
            # Pattern: ID_HOSPITAL-NAME_standardcharges
            # Example: 74-2781812_ST-DAVIDS-MEDICAL-CENTER_standardcharges
            parts = filename.split('_')
            if len(parts) >= 2:
                # Extract facility ID (first part)
                facility_id = parts[0]
                metadata['facility_id'] = facility_id
                
                # Extract hospital name (second part, replace hyphens with spaces)
                hospital_name = parts[1].replace('-', ' ').title()
                metadata['facility_name'] = hospital_name
                
                # Note: Location must be extracted from file content or provided explicitly
                # No default location values will be set
                
                logger.info(f"Extracted metadata from filename: {facility_id} - {hospital_name}")
                
        except Exception as e:
            logger.warning(f"Could not extract metadata from filename {filename}: {e}")
        
        return metadata
    
    def _map_json_metadata(self, json_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Map JSON metadata fields to database fields."""
        mapped = {}
        
        # Map hospital_name to facility_name
        if 'hospital_name' in json_metadata:
            mapped['facility_name'] = json_metadata['hospital_name']
        
        # Map hospital_address to address and extract city/state
        if 'hospital_address' in json_metadata:
            address = json_metadata['hospital_address']
            if isinstance(address, list) and len(address) > 0:
                address_str = address[0]
            else:
                address_str = str(address)
            
            mapped['address'] = address_str
            
            # Extract city and state from address
            address_parts = address_str.split(',')
            if len(address_parts) >= 3:
                # Format: "STREET, CITY, STATE ZIP"
                mapped['city'] = address_parts[1].strip()
                state_zip_part = address_parts[2].strip()
                # Extract state (usually 2 letters)
                import re
                state_match = re.search(r'\b([A-Z]{2})\b', state_zip_part)
                if state_match:
                    mapped['state'] = state_match.group(1)
        
        # Map last_updated_on to last_updated
        if 'last_updated_on' in json_metadata:
            mapped['last_updated'] = json_metadata['last_updated_on']
        
        # Map version to file_version
        if 'version' in json_metadata:
            mapped['file_version'] = json_metadata['version']
        
        logger.info(f"Mapped JSON metadata: {mapped}")
        return mapped
    
    async def _create_hospital_record(self, metadata: Dict[str, Any]) -> Hospital:
        """Create or update hospital record using the configured output writer."""
        try:
            hospital = Hospital(**metadata)

            # Prepare hospital data
            hospital_data = {
                'facility_id': hospital.facility_id,
                'facility_name': hospital.facility_name,
                'city': hospital.city,
                'state': hospital.state,
                'address': hospital.address,
                'source_url': hospital.source_url,
                'file_version': hospital.file_version,
                'last_updated': hospital.last_updated,
                'ingested_at': hospital.ingested_at.isoformat()
            }

            # Write using output writer
            if self.writer:
                self.writer.write_hospital(hospital_data)
                logger.info(f"Wrote hospital record: {hospital.facility_id}")
            else:
                logger.warning("No output writer configured, skipping hospital write")

            return hospital

        except Exception as e:
            logger.error(f"Failed to create hospital record: {e}")
            raise
    
    async def process_directory(self, directory_path: Union[str, Path], 
                              pattern: str = "*.csv") -> List[DataIngestionResult]:
        """
        Process all files in a directory matching the given pattern.
        
        Args:
            directory_path: Path to directory containing data files
            pattern: File pattern to match (e.g., "*.csv", "*.json")
            
        Returns:
            List of DataIngestionResult objects
        """
        directory_path = Path(directory_path)
        if not directory_path.exists():
            raise FileNotFoundError(f"Directory not found: {directory_path}")
        
        # Find all matching files
        files = list(directory_path.glob(pattern))
        logger.info(f"Found {len(files)} files to process in {directory_path}")
        
        # Process files concurrently
        tasks = []
        for file_path in files:
            task = self.process_file(file_path)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter out exceptions and return results
        valid_results = []
        for result in results:
            if isinstance(result, DataIngestionResult):
                valid_results.append(result)
            else:
                logger.error(f"File processing failed: {result}")
        
        return valid_results
    
    def close(self):
        """Close the processor and cleanup resources."""
        if self.writer:
            self.writer.close()
        if self.executor:
            self.executor.shutdown(wait=True)
            logger.info("Data processor closed")
