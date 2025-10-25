"""
JSON-specific data processing logic for medical pricing data.
Handles JSON file parsing, data extraction, and normalization.
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

from .models import MedicalOperation
from .streaming_utils import (
    stream_json_array, is_standardized_code_type
)

logger = logging.getLogger(__name__)


class JSONProcessor:
    """JSON-specific data processing class."""
    
    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
    
    async def process_json_file(self, file_path: Path, facility_id: str) -> Dict[str, Any]:
        """Process JSON file using advanced streaming patterns."""
        successful_records = 0
        failed_records = 0
        errors = []
        batch_operations = []
        items_processed = 0
        
        try:
            logger.info(f"Starting JSON processing for {file_path}")
            # Use streaming JSON array processing
            for item in stream_json_array(file_path, "standard_charge_information"):
                items_processed += 1
                if items_processed % 1000 == 0:
                    logger.info(f"Processed {items_processed} JSON items so far")
                
                try:
                    # Handle case where item is the entire array
                    if isinstance(item, list):
                        for sub_item in item:
                            operations = self._parse_json_item(sub_item, facility_id)
                            for operation in operations:
                                if operation:
                                    batch_operations.append(operation.dict())
                                    successful_records += 1
                                    
                                    # Process batch when it reaches batch_size
                                    if len(batch_operations) >= self.batch_size:
                                        await self._batch_insert_operations(batch_operations)
                                        batch_operations = []
                                else:
                                    failed_records += 1
                    else:
                        operations = self._parse_json_item(item, facility_id)
                        for operation in operations:
                            if operation:
                                batch_operations.append(operation.dict())
                                successful_records += 1
                                
                                # Process batch when it reaches batch_size
                                if len(batch_operations) >= self.batch_size:
                                    await self._batch_insert_operations(batch_operations)
                                    batch_operations = []
                            else:
                                failed_records += 1
                                
                except Exception as e:
                    failed_records += 1
                    errors.append(f"JSON item processing error: {str(e)}")
                    logger.warning(f"Error processing JSON item {items_processed}: {e}")
                
        except Exception as e:
            logger.error(f"Error processing JSON file {file_path}: {e}")
            errors.append(f"File processing error: {str(e)}")
        
        logger.info(f"JSON processing complete. Items processed: {items_processed}, Successful: {successful_records}, Failed: {failed_records}")
        
        # Process remaining batch
        if batch_operations:
            await self._batch_insert_operations(batch_operations)
        
        return {
            'total_records': successful_records + failed_records,
            'successful_records': successful_records,
            'failed_records': failed_records,
            'errors': errors
        }
    
    def _parse_json_item(self, item: Dict[str, Any], facility_id: str) -> List[MedicalOperation]:
        """Parse JSON item using advanced patterns."""
        operations = []
        
        try:
            # Ensure item is a dictionary
            if not isinstance(item, dict):
                return operations
            
            # Filter: only process outpatient records
            setting = self._extract_setting_from_item(item)
            if setting and setting != 'outpatient':
                return operations
                
            description = item.get("description", "")
            prices = self._extract_prices_from_item(item)

            # Require a positive cash price
            cash_price = prices.get("cash_price")
            if cash_price is None or cash_price <= 0:
                return operations
            
            # Extract all codes and group by type
            codes_dict = {}
            for code_info in self._extract_codes_from_item(item):
                # Skip if no code
                if not code_info.get("code"):
                    continue
                
                code_type = code_info.get("code_type", "unknown")
                code_value = code_info.get("code")

                if code_type not in codes_dict:
                    codes_dict[code_type] = []
                codes_dict[code_type].append(code_value)
            
            # Skip if no codes found
            if not codes_dict:
                return operations
            
            # Create price record
            price_record = {
                "facility_id": facility_id,
                "codes": codes_dict,
                "description": description,
                "currency": "USD"
            }
            
            # Convert Decimal prices to float for JSON serialization
            for price_key, price_value in prices.items():
                if price_value is not None:
                    price_record[price_key] = float(price_value)
                else:
                    price_record[price_key] = None
            
            # Validate with Pydantic schema
            try:
                operation = MedicalOperation(**price_record)
                operations.append(operation)
            except Exception as e:
                logger.warning(f"JSON item validation failed: {e}")
                
        except Exception as e:
            logger.warning(f"Error parsing JSON item: {e}")
        
        return operations

    def _extract_codes_from_item(self, item: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract medical codes from a charge item."""
        codes = []
        code_info = item.get("code_information", [])
        
        for code_data in code_info:
            # Handle case where code_data might be a list or other type
            if isinstance(code_data, dict):
                code_type = code_data.get("type", "unknown")
                code = code_data.get("code", "")
                
                # Only yield standardized code types
                if is_standardized_code_type(code_type):
                    codes.append({
                        "code_type": code_type,
                        "code": code
                    })
        
        return codes

    def _extract_prices_from_item(self, item: Dict[str, Any]) -> Dict[str, Optional[float]]:
        """Extract price information from a charge item."""
        prices = {
            "cash_price": None,
            "gross_charge": None,
            "negotiated_min": None,
            "negotiated_max": None
        }
        
        # Get the first standard charge entry (most hospitals have one)
        standard_charges = item.get("standard_charges", [])
        if standard_charges:
            charge = standard_charges[0]
            
            # Map common fields to our canonical schema
            if "discounted_cash" in charge:
                prices["cash_price"] = float(charge["discounted_cash"])
            if "gross_charge" in charge:
                prices["gross_charge"] = float(charge["gross_charge"])
            if "minimum" in charge:
                prices["negotiated_min"] = float(charge["minimum"])
            if "maximum" in charge:
                prices["negotiated_max"] = float(charge["maximum"])
        
        return prices

    def _extract_setting_from_item(self, item: Dict[str, Any]) -> Optional[str]:
        """Extract setting (inpatient/outpatient) from a charge item."""
        standard_charges = item.get("standard_charges", [])
        if standard_charges:
            charge = standard_charges[0]
            setting = charge.get("setting", "").strip().lower()
            return setting if setting else None
        return None
    
    async def _batch_insert_operations(self, operations: List[Dict[str, Any]]):
        """Insert a batch of medical operations into the database."""
        if not operations:
            return
        
        try:
            from .database import db_manager
            
            with db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Prepare batch insert
                    insert_query = """
                        INSERT INTO medical_operations (
                            facility_id, codes, description, cash_price, gross_charge,
                            negotiated_min, negotiated_max, currency, ingested_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    values = []
                    for op in operations:
                        values.append((
                            op['facility_id'],
                            json.dumps(op['codes']),
                            op['description'],
                            op['cash_price'],
                            op['gross_charge'],
                            op['negotiated_min'],
                            op['negotiated_max'],
                            op['currency'],
                            op['ingested_at']
                        ))
                    
                    cursor.executemany(insert_query, values)
                    conn.commit()
                    
                    logger.info(f"Inserted {len(operations)} medical operations")
                    
        except Exception as e:
            logger.error(f"Failed to batch insert operations: {e}")
            raise
