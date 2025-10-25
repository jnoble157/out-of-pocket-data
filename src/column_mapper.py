"""
Flexible column mapping for hospital CSV files with varying formats.
Based on patterns from temp/pipelines for handling different hospital data formats.
"""
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
import re
import logging

logger = logging.getLogger(__name__)


@dataclass
class ColumnMapping:
    """Maps actual CSV column names to our canonical fields."""
    description: Optional[str] = None
    code_columns: List[Dict[str, str]] = None  # List of (code, type) column pairs
    cash_price: Optional[str] = None
    gross_charge: Optional[str] = None
    min_negotiated: Optional[str] = None
    max_negotiated: Optional[str] = None
    setting: Optional[str] = None
    modifiers: Optional[str] = None
    drug_unit: Optional[str] = None
    
    def __post_init__(self):
        if self.code_columns is None:
            self.code_columns = []


class CSVColumnMapper:
    """
    Intelligently maps varying hospital CSV column names to canonical fields.
    
    Uses pattern matching, fuzzy string matching, and common variations
    to handle different hospital formats.
    """
    
    # Pattern libraries for fuzzy matching
    DESCRIPTION_PATTERNS = [
        'description',
        'service_description',
        'procedure_description',
        'item_description',
        'charge_description',
        'cpt_description',
        'service',
        'procedure'
    ]
    
    CASH_PRICE_PATTERNS = [
        'standard_charge|discounted_cash',
        'discounted_cash',
        'cash_price',
        'self_pay',
        'selfpay',
        'cash_discount',
        'discounted_price',
        'standard_charge_discounted_cash',
        'cash',
        'de_identified_minimum'  # Sometimes cash is in min column
    ]
    
    GROSS_CHARGE_PATTERNS = [
        'standard_charge|gross',
        'gross',
        'gross_charge',
        'standard_charge',
        'charge_amount',
        'list_price',
        'standard_charge_gross',
        'gross_price'
    ]
    
    MIN_NEGOTIATED_PATTERNS = [
        'standard_charge|min',
        'min',
        'minimum',
        'negotiated_min',
        'min_negotiated',
        'deidentified_min',
        'de_identified_min',
        'standard_charge_min',
        'min_price'
    ]
    
    MAX_NEGOTIATED_PATTERNS = [
        'standard_charge|max',
        'max',
        'maximum',
        'negotiated_max',
        'max_negotiated',
        'deidentified_max',
        'de_identified_max',
        'standard_charge_max',
        'max_price'
    ]
    
    SETTING_PATTERNS = [
        'setting',
        'patient_setting',
        'care_setting',
        'service_setting',
        'location',
        'place_of_service'
    ]
    
    # Code column patterns - handle various formats
    CODE_PATTERNS = {
        # Format: code|1, code|2, code|3, code|1|type, code|2|type
        'pipe_numbered': r'^code\|(\d+)$',
        'pipe_numbered_type': r'^code\|(\d+)\|type$',
        
        # Format: code_1, code_2, code_3, code_1_type, code_2_type
        'underscore_numbered': r'^code_(\d+)$',
        'underscore_numbered_type': r'^code_(\d+)_type$',
        
        # Format: billing_code, billing_code_type, billing_code_type_1
        'billing_code': r'^billing_code(_\d+)?$',
        'billing_code_type': r'^billing_code_type(_\d+)?$',
        
        # Format: cpt_code, hcpcs_code, drg_code
        'code_type_specific': r'^(cpt|hcpcs|drg|icd10|rev|ndc)_code(_\d+)?$',
        
        # Format: code_information (JSON or nested structure)
        'code_information': r'^code_information',
        
        # Simple variations
        'simple_code': r'^code$',
        'simple_type': r'^(code_type|type)$',
    }
    
    def __init__(self, columns: List[str], fuzzy_threshold: int = 80):
        """
        Initialize mapper with actual CSV column names.
        
        Args:
            columns: List of actual column names from CSV
            fuzzy_threshold: Minimum similarity score for fuzzy matching (0-100)
        """
        self.columns = columns
        self.fuzzy_threshold = fuzzy_threshold
        self.mapping = self._build_mapping()
    
    def _build_mapping(self) -> ColumnMapping:
        """Build column mapping using pattern matching and fuzzy matching."""
        mapping = ColumnMapping()
        
        # Map standard fields
        mapping.description = self._find_best_match(self.DESCRIPTION_PATTERNS)
        mapping.cash_price = self._find_best_match(self.CASH_PRICE_PATTERNS)
        mapping.gross_charge = self._find_best_match(self.GROSS_CHARGE_PATTERNS)
        mapping.min_negotiated = self._find_best_match(self.MIN_NEGOTIATED_PATTERNS)
        mapping.max_negotiated = self._find_best_match(self.MAX_NEGOTIATED_PATTERNS)
        mapping.setting = self._find_best_match(self.SETTING_PATTERNS)
        
        # Map code columns (more complex - can be multiple)
        mapping.code_columns = self._find_code_columns()
        
        return mapping
    
    def _find_best_match(self, patterns: List[str]) -> Optional[str]:
        """
        Find best matching column using fuzzy string matching.
        
        Args:
            patterns: List of pattern strings to match against
            
        Returns:
            Best matching column name, or None if no good match
        """
        # Normalize column names for comparison
        normalized_columns = {col.lower().replace(' ', '_'): col for col in self.columns}
        
        # Try exact matches first
        for pattern in patterns:
            pattern_lower = pattern.lower()
            if pattern_lower in normalized_columns:
                return normalized_columns[pattern_lower]
        
        # Try fuzzy matching using simple string similarity
        best_match = None
        best_score = 0
        
        for pattern in patterns:
            pattern_lower = pattern.lower()
            
            for col_lower, col_original in normalized_columns.items():
                # Simple similarity calculation
                similarity = self._calculate_similarity(pattern_lower, col_lower)
                
                if similarity >= self.fuzzy_threshold and similarity > best_score:
                    best_score = similarity
                    best_match = col_original
        
        return best_match
    
    def _calculate_similarity(self, pattern: str, column: str) -> int:
        """
        Calculate similarity between pattern and column name.
        
        Args:
            pattern: Pattern to match
            column: Column name to match against
            
        Returns:
            Similarity score (0-100)
        """
        # Simple similarity based on common substrings
        pattern_words = set(pattern.split('_'))
        column_words = set(column.split('_'))
        
        if not pattern_words or not column_words:
            return 0
        
        # Calculate Jaccard similarity
        intersection = len(pattern_words.intersection(column_words))
        union = len(pattern_words.union(column_words))
        
        if union == 0:
            return 0
        
        jaccard = intersection / union
        
        # Also check for substring matches
        substring_match = 0
        for word in pattern_words:
            if any(word in col_word for col_word in column_words):
                substring_match += 1
        
        substring_score = substring_match / len(pattern_words) if pattern_words else 0
        
        # Combine scores
        final_score = (jaccard * 0.7 + substring_score * 0.3) * 100
        
        return int(final_score)
    
    def _find_code_columns(self) -> List[Dict[str, str]]:
        """
        Find all code-related columns and group them by code instance.
        
        Returns:
            List of dicts with 'code' and 'type' column names
            Example: [
                {'code': 'code|1', 'type': 'code|1|type'},
                {'code': 'code|2', 'type': 'code|2|type'},
                {'code': 'billing_code', 'type': 'billing_code_type'}
            ]
        """
        code_groups = {}
        
        for col in self.columns:
            col_lower = col.lower()
            
            # Try each pattern
            for pattern_name, pattern_regex in self.CODE_PATTERNS.items():
                match = re.match(pattern_regex, col_lower)
                
                if match:
                    # Extract instance number (if numbered)
                    instance = match.group(1) if match.groups() else '0'
                    
                    # Determine if this is a code or type column
                    is_type = 'type' in pattern_name or 'type' in col_lower
                    
                    # Group by instance
                    if instance not in code_groups:
                        code_groups[instance] = {'code': None, 'type': None}
                    
                    if is_type:
                        code_groups[instance]['type'] = col
                    else:
                        code_groups[instance]['code'] = col
                    
                    break
        
        # Convert to list, filtering out incomplete pairs
        result = []
        for instance in sorted(code_groups.keys()):
            group = code_groups[instance]
            if group['code']:  # At minimum, must have a code column
                result.append(group)
        
        return result
    
    def get_mapping(self) -> ColumnMapping:
        """Get the built column mapping."""
        return self.mapping
    
    def validate_mapping(self) -> Dict[str, bool]:
        """
        Validate that essential columns were found.
        
        Returns:
            Dict of field name -> found (bool)
        """
        return {
            'description': self.mapping.description is not None,
            'has_codes': len(self.mapping.code_columns) > 0,
            'has_price': (
                self.mapping.cash_price is not None or
                self.mapping.gross_charge is not None
            ),
            'cash_price': self.mapping.cash_price is not None,
            'gross_charge': self.mapping.gross_charge is not None,
            'min_negotiated': self.mapping.min_negotiated is not None,
            'max_negotiated': self.mapping.max_negotiated is not None,
        }
    
    def get_missing_fields(self) -> List[str]:
        """Get list of critical fields that couldn't be mapped."""
        validation = self.validate_mapping()
        missing = []
        
        if not validation['description']:
            missing.append('description')
        if not validation['has_codes']:
            missing.append('code columns')
        if not validation['has_price']:
            missing.append('pricing information')
        
        return missing
