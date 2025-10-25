"""
CSV-specific data processing logic for medical pricing data.
Handles CSV file parsing, column mapping, and data extraction.
"""
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

from .models import MedicalOperation
from .streaming_utils import (
    stream_csv_rows, detect_metadata_rows, safe_decimal, is_standardized_code_type
)
from .column_mapper import CSVColumnMapper

logger = logging.getLogger(__name__)


class CSVProcessor:
    """CSV-specific data processing class."""
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
    
    async def process_csv_file(self, file_path: Path, facility_id: str) -> Dict[str, Any]:
        """Process CSV file using advanced streaming patterns with deduplication."""
        successful_records = 0
        failed_records = 0
        errors = []
        all_operations = []  # Collect all operations for deduplication
        
        try:
            # Detect metadata rows to skip
            metadata_rows = detect_metadata_rows(file_path)
            
            # Build column mapping using proper CSV parsing
            import csv
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                reader = csv.reader(f)
                
                # Skip metadata rows
                for _ in range(metadata_rows):
                    next(reader)
                
                # Read header
                columns = next(reader)
                columns = [col.strip() for col in columns]
            
            # Create column mapper
            mapper = CSVColumnMapper(columns)
            mapping = mapper.get_mapping()
            
            # Validate mapping
            missing_fields = mapper.get_missing_fields()
            if missing_fields:
                logger.warning("Missing critical fields in CSV: %s", missing_fields)
            
            # Process CSV rows using streaming - collect all operations first
            for row in stream_csv_rows(file_path, metadata_rows):
                try:
                    operation = self._parse_csv_row_with_mapping(row, facility_id, mapping)
                    if operation:
                        all_operations.append(operation.dict())
                    else:
                        failed_records += 1
                except Exception as e:
                    failed_records += 1
                    errors.append(f"Row processing error: {str(e)}")
            
            # Deduplicate operations
            logger.info("Before deduplication: %d operations", len(all_operations))
            deduplicated_operations = self._deduplicate_operations(all_operations)
            logger.info("After deduplication: %d operations", len(deduplicated_operations))
            
            # Process deduplicated operations in batches
            batch_operations = []
            for operation in deduplicated_operations:
                batch_operations.append(operation)
                successful_records += 1
                
                # Process batch when it reaches batch_size
                if len(batch_operations) >= self.batch_size:
                    await self._batch_insert_operations(batch_operations)
                    batch_operations = []
            
            # Process remaining batch
            if batch_operations:
                await self._batch_insert_operations(batch_operations)
                
        except Exception as e:
            logger.error("Error processing CSV file %s: %s", file_path, e)
            errors.append(f"File processing error: {str(e)}")
        
        return {
            'total_records': successful_records + failed_records,
            'successful_records': successful_records,
            'failed_records': failed_records,
            'errors': errors
        }
    
    def _parse_csv_row_with_mapping(self, row: Dict[str, str], facility_id: str, mapping) -> Optional[MedicalOperation]:
        """Parse CSV row using advanced column mapping."""
        try:
            # Extract codes using mapping
            codes = {}
            for code_group in mapping.code_columns:
                code_col = code_group.get('code')
                type_col = code_group.get('type')
                
                if not code_col:
                    continue
                
                code_value = row.get(code_col, '').strip()
                if not code_value or code_value.upper() in ('N/A', 'NULL', 'NONE'):
                    continue
                
                # Get code type
                code_type = 'unknown'
                if type_col:
                    code_type_value = row.get(type_col, '').strip().upper()
                    if code_type_value:
                        # Normalize code types - only standardized codes
                        # CPT codes are stored under HCPCS since CPT is a subset of HCPCS
                        type_mapping = {
                            'CPT': 'HCPCS',  # CPT codes stored under HCPCS
                            'HCPCS': 'HCPCS',
                            'RC': 'RC',
                            'REV': 'RC',  # Revenue Code alternative
                            'ICD10': 'ICD-10',
                            'ICD-10': 'ICD-10',
                            'ICD-10-CM': 'ICD-10-CM',
                            'ICD-10-PCS': 'ICD-10-PCS',
                            'ICD10CM': 'ICD-10-CM',
                            'ICD10PCS': 'ICD-10-PCS'
                        }
                        code_type = type_mapping.get(code_type_value, code_type_value)
                
                # Only include standardized code types
                if is_standardized_code_type(code_type):
                    if code_type not in codes:
                        codes[code_type] = []
                    codes[code_type].append(code_value)
            
            if not codes:
                return None
            
            # Extract price information using mapping
            cash_price = None
            if mapping.cash_price:
                cash_price = safe_decimal(row.get(mapping.cash_price))
            
            gross_charge = None
            if mapping.gross_charge:
                gross_charge = safe_decimal(row.get(mapping.gross_charge))
            
            negotiated_min = None
            if mapping.min_negotiated:
                negotiated_min = safe_decimal(row.get(mapping.min_negotiated))
            
            negotiated_max = None
            if mapping.max_negotiated:
                negotiated_max = safe_decimal(row.get(mapping.max_negotiated))
            
            # Require a positive cash price
            if not cash_price:
                return None
            
            # Extract description
            description = 'Unknown'
            if mapping.description:
                desc_value = row.get(mapping.description, '').strip()
                if desc_value:
                    description = desc_value
            
            # Extract setting
            setting = None
            if mapping.setting:
                setting = row.get(mapping.setting, '').strip().lower()
            
            # Filter: only process outpatient records
            if setting and setting != 'outpatient':
                return None
            
            # Build price record
            price_record = {
                'facility_id': facility_id,
                'codes': codes,
                'description': description,
                'cash_price': float(cash_price),
                'gross_charge': float(gross_charge) if gross_charge else None,
                'negotiated_min': float(negotiated_min) if negotiated_min else None,
                'negotiated_max': float(negotiated_max) if negotiated_max else None,
                'currency': 'USD'
            }
            
            # Validate with Pydantic
            return MedicalOperation(**price_record)
            
        except Exception as e:
            logger.warning("Error parsing CSV row: %s", e)
            return None
    
    async def _batch_insert_operations(self, operations: List[Dict[str, Any]]):
        """Insert a batch of medical operations into the database."""
        if not operations:
            return
        
        try:
            from .database import supabase_manager
            
            # Initialize Supabase if not already done
            if not supabase_manager.client:
                supabase_manager.initialize()
            
            # Prepare operations data for Supabase
            operations_data = []
            for op in operations:
                operations_data.append({
                    'facility_id': op['facility_id'],
                    'codes': op['codes'],  # Supabase handles JSON automatically
                    'rc_code': op.get('rc_code'),
                    'hcpcs_code': op.get('hcpcs_code'),
                    'description': op['description'],
                    'cash_price': op['cash_price'],
                    'gross_charge': op['gross_charge'],
                    'negotiated_min': op['negotiated_min'],
                    'negotiated_max': op['negotiated_max'],
                    'currency': op['currency'],
                    'ingested_at': op['ingested_at'].isoformat()
                })
            
            # Insert using Supabase client
            supabase_manager.batch_insert_medical_operations(operations_data)
            
            logger.info("Inserted %d medical operations", len(operations))
                    
        except Exception as e:
            logger.error("Failed to batch insert operations: %s", e)
            raise
    
    def _deduplicate_operations(self, operations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Deduplicate operations by grouping on core attributes and keeping the best record.
        
        Groups operations by: description, codes, and setting.
        For each group, keeps the operation with the best cash price (non-null, highest value).
        
        Args:
            operations: List of operation dictionaries to deduplicate
            
        Returns:
            List of deduplicated operations
        """
        # Group operations by core attributes
        groups = {}
        
        for operation in operations:
            # Create a key based on core attributes (excluding pricing)
            key_parts = [
                operation.get('description', ''),
                str(sorted(operation.get('codes', {}).items()))  # Sort for consistent comparison
            ]
            key = '|'.join(key_parts)
            
            if key not in groups:
                groups[key] = []
            groups[key].append(operation)
        
        # For each group, select the best operation
        deduplicated = []
        for group_operations in groups.values():
            if not group_operations:
                continue
                
            # If only one operation, keep it
            if len(group_operations) == 1:
                deduplicated.append(group_operations[0])
                continue
            
            # Multiple operations - select the best one
            best_operation = self._select_best_operation(group_operations)
            if best_operation:
                deduplicated.append(best_operation)
        
        logger.info("Deduplicated %d operations to %d unique operations", len(operations), len(deduplicated))
        return deduplicated
    
    def _select_best_operation(self, operations: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Select the best operation from a group of duplicates.
        
        Priority:
        1. Operation with cash_price (prefer non-null)
        2. Operation with highest cash_price
        3. Operation with gross_charge as fallback
        4. For negotiated prices, use the biggest range (min to max)
        5. First operation if all else equal
        
        Args:
            operations: List of duplicate operations
            
        Returns:
            Best operation with merged negotiated price ranges
        """
        if not operations:
            return None
        
        # Score each operation
        scored_operations = []
        for operation in operations:
            score = 0
            
            # Prefer operations with cash_price
            cash_price = operation.get('cash_price')
            if cash_price is not None:
                score += 1000  # Base score for having cash_price
                score += cash_price  # Higher cash price = higher score
            else:
                # Fallback to gross_charge
                gross_charge = operation.get('gross_charge')
                if gross_charge is not None:
                    score += 500  # Lower base score for gross_charge
                    score += gross_charge * 0.1  # Much lower weight for gross_charge
            
            scored_operations.append((score, operation))
        
        # Sort by score (descending) and get the best operation
        scored_operations.sort(key=lambda x: x[0], reverse=True)
        best_operation = scored_operations[0][1].copy()
        
        # Merge negotiated price ranges from all operations
        negotiated_min_values = []
        negotiated_max_values = []
        
        for operation in operations:
            negotiated_min = operation.get('negotiated_min')
            negotiated_max = operation.get('negotiated_max')
            
            if negotiated_min is not None:
                negotiated_min_values.append(negotiated_min)
            if negotiated_max is not None:
                negotiated_max_values.append(negotiated_max)
        
        # Use the biggest range (min of all mins, max of all maxes)
        if negotiated_min_values:
            best_operation['negotiated_min'] = min(negotiated_min_values)
        if negotiated_max_values:
            best_operation['negotiated_max'] = max(negotiated_max_values)
        
        return best_operation
