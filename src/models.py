"""
Pydantic models for medical pricing data structures.
Based on patterns from temp/pipelines for standardized data processing.
"""
from datetime import datetime
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, validator
import re


class Hospital(BaseModel):
    """Model for hospital metadata - matches FacilityRecord from temp pipelines."""
    facility_id: str = Field(..., description="Unique facility identifier")
    facility_name: str = Field(..., description="Hospital name")
    city: str = Field(..., description="City where hospital is located")
    state: str = Field(..., description="State where hospital is located")
    address: Optional[str] = Field(None, description="Full address of the hospital")
    source_url: str = Field(..., description="URL or path to source data file")
    file_version: Optional[str] = Field(None, description="Version of the data file")
    last_updated: Optional[str] = Field(None, description="Last update date from source")
    ingested_at: datetime = Field(default_factory=datetime.utcnow, description="When data was ingested")
    
    @validator('facility_id')
    def validate_facility_id(cls, v):
        """Ensure facility_id follows the expected format."""
        if not re.match(r'^[a-z0-9-]+$', v):
            raise ValueError('facility_id must be lowercase alphanumeric with hyphens only')
        return v
    
    @validator('state')
    def validate_state(cls, v):
        """Ensure state is uppercase 2-letter code."""
        if len(v) != 2 or not v.isupper():
            raise ValueError('State must be a 2-letter uppercase code')
        return v


class MedicalOperation(BaseModel):
    """Model for medical operation pricing data - matches PriceRecord from temp pipelines."""
    facility_id: str = Field(..., description="Foreign key to hospital")
    codes: Dict[str, List[str]] = Field(..., description="Medical codes (CPT, RC, etc.)")
    description: str = Field(..., description="Description of the medical operation")
    cash_price: Optional[float] = Field(None, ge=0, description="Cash price for the operation")
    gross_charge: Optional[float] = Field(None, ge=0, description="Gross charge amount")
    negotiated_min: Optional[float] = Field(None, ge=0, description="Minimum negotiated price")
    negotiated_max: Optional[float] = Field(None, ge=0, description="Maximum negotiated price")
    currency: str = Field(default="USD", description="Currency code")
    ingested_at: datetime = Field(default_factory=datetime.utcnow, description="When data was ingested")
    
    @validator('codes')
    def validate_codes(cls, v):
        """Ensure codes dictionary has valid structure."""
        if not isinstance(v, dict):
            raise ValueError('Codes must be a dictionary')
        for code_type, code_list in v.items():
            if not isinstance(code_list, list):
                raise ValueError(f'Code values for {code_type} must be a list')
            if not all(isinstance(code, str) for code in code_list):
                raise ValueError(f'All codes for {code_type} must be strings')
        return v
    
    @validator('currency')
    def validate_currency(cls, v):
        """Ensure currency is a valid 3-letter code."""
        if len(v) != 3 or not v.isupper():
            raise ValueError('Currency must be a 3-letter uppercase code')
        return v
    
    @validator('negotiated_min', 'negotiated_max')
    def validate_negotiated_prices(cls, v, values):
        """Ensure negotiated prices are logical."""
        if 'negotiated_min' in values and 'negotiated_max' in values:
            if values['negotiated_min'] > values['negotiated_max']:
                raise ValueError('negotiated_min cannot be greater than negotiated_max')
        return v


class DataIngestionResult(BaseModel):
    """Model for tracking data ingestion results."""
    facility_id: str
    total_records: int
    successful_records: int
    failed_records: int
    errors: List[str] = Field(default_factory=list)
    processing_time: float
    ingested_at: datetime = Field(default_factory=datetime.utcnow)


class NormalizedPriceRow(BaseModel):
    """Normalized price row for processing - matches temp pipeline patterns."""
    facility_id: str
    codes: Dict[str, List[str]]
    description: str
    cash_price: Optional[float] = None
    gross_charge: Optional[float] = None
    negotiated_min: Optional[float] = None
    negotiated_max: Optional[float] = None
    currency: str = "USD"
    setting: Optional[str] = None
    modifiers: Optional[str] = None
    drug_unit: Optional[str] = None


class FacilityRecord(BaseModel):
    """Facility record - alias for Hospital to match temp pipeline naming."""
    facility_id: str
    facility_name: str
    city: str
    state: str
    address: Optional[str] = None
    source_url: str
    file_version: Optional[str] = None
    last_updated: Optional[str] = None
    ingested_at: datetime = Field(default_factory=datetime.utcnow)


class PriceRecord(BaseModel):
    """Price record - alias for MedicalOperation to match temp pipeline naming."""
    facility_id: str
    codes: Dict[str, List[str]]
    description: str
    cash_price: Optional[float] = None
    gross_charge: Optional[float] = None
    negotiated_min: Optional[float] = None
    negotiated_max: Optional[float] = None
    currency: str = "USD"
    ingested_at: datetime = Field(default_factory=datetime.utcnow)


def generate_facility_id(hospital_name: str, facility_number: str = None) -> str:
    """
    Generate a standardized facility_id from hospital name.
    
    Args:
        hospital_name: Name of the hospital
        facility_number: Optional facility number (will be generated if not provided)
    
    Returns:
        Standardized facility_id in format: abbreviated-hospital-code
    """
    # Create a consistent abbreviation from hospital name
    abbreviation = _create_hospital_abbreviation(hospital_name)
    
    return abbreviation


def _create_hospital_abbreviation(hospital_name: str) -> str:
    """
    Create a consistent abbreviation from hospital name.
    
    Args:
        hospital_name: Name of the hospital
    
    Returns:
        Abbreviated hospital code (e.g., "bsw-cedar-park", "ascension-seton")
    """
    # Clean and normalize hospital name
    clean_name = re.sub(r'[^a-z0-9\s-]', '', hospital_name.lower())
    clean_name = re.sub(r'\s+', ' ', clean_name.strip())
    
    # Handle common hospital name patterns
    words = clean_name.split()
    
    # Common hospital name mappings for consistency
    hospital_mappings = {
        'baylor': 'bsw',
        'scott': 'bsw', 
        'white': 'bsw',
        'ascension': 'asc',
        'seton': 'asc',
        'cedar': 'cp',
        'park': 'cp',
        'regional': 'reg',
        'medical': 'med',
        'center': 'ctr',
        'hospital': 'hosp',
        'health': 'hlth',
        'system': 'sys',
        'emergency': 'er',
        'campus': 'campus',
        'georgetown': 'gtown'
    }
    
    # Build abbreviation with deduplication
    abbreviation_parts = []
    seen_parts = set()
    
    for word in words:
        if word in hospital_mappings:
            mapped_word = hospital_mappings[word]
            if mapped_word not in seen_parts:
                abbreviation_parts.append(mapped_word)
                seen_parts.add(mapped_word)
        elif len(word) > 3:
            # Take first 3-4 characters of longer words
            short_word = word[:4]
            if short_word not in seen_parts:
                abbreviation_parts.append(short_word)
                seen_parts.add(short_word)
        else:
            if word not in seen_parts:
                abbreviation_parts.append(word)
                seen_parts.add(word)
    
    # Join with hyphens
    abbreviation = '-'.join(abbreviation_parts)
    
    # Remove consecutive hyphens
    abbreviation = re.sub(r'-+', '-', abbreviation)
    
    # Remove leading/trailing hyphens
    abbreviation = abbreviation.strip('-')
    
    return abbreviation
