#!/usr/bin/env python3
"""
Find medical operations by standardized codes (CPT, ICD-10, HCPCS, etc.)
"""
import sys
import os
import json
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Add the parent directory to the path so we can import from src
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import supabase_manager

# Load environment variables
load_dotenv('.supabase.env')

def find_by_codes(
    cpt_codes: Optional[List[str]] = None,
    icd10_codes: Optional[List[str]] = None,
    hcpcs_codes: Optional[List[str]] = None,
    rc_codes: Optional[List[str]] = None,
    facility_id: Optional[str] = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Find medical operations by standardized codes, ordered by number of matches.
    
    Args:
        cpt_codes: List of CPT codes to search for
        icd10_codes: List of ICD-10 codes to search for
        hcpcs_codes: List of HCPCS codes to search for
        rc_codes: List of Revenue Codes to search for
        facility_id: Filter by specific facility ID
        limit: Maximum number of results to return
    
    Returns:
        List of matching medical operations ordered by match count (highest first)
    """
    try:
        # Initialize Supabase client
        supabase_manager.initialize()
        
        # Get all requested codes
        all_codes = []
        if cpt_codes:
            all_codes.extend([('CPT', code) for code in cpt_codes])
        if icd10_codes:
            all_codes.extend([('ICD10', code) for code in icd10_codes])
        if hcpcs_codes:
            all_codes.extend([('HCPCS', code) for code in hcpcs_codes])
        if rc_codes:
            all_codes.extend([('RC', code) for code in rc_codes])
        
        if not all_codes:
            return []
        
        # Build query to get all records that match any of the codes
        query = supabase_manager.client.table('medical_operations').select(
            'facility_id, description, cash_price, gross_charge, negotiated_min, negotiated_max, codes, currency, ingested_at, hospitals!inner(facility_name, city, state)'
        )
        
        # Apply facility filter if specified
        if facility_id:
            query = query.eq('facility_id', facility_id)
        
        # Apply OR condition for any matching codes
        or_conditions = []
        for code_type, code in all_codes:
            or_conditions.append({'codes': {code_type: [code]}})
        
        # Execute query to get all potential matches
        result = query.execute()
        
        if not result.data:
            return []
        
        # Calculate match scores and filter
        scored_results = []
        for record in result.data:
            match_count = 0
            non_rc_matches = 0
            codes = record.get('codes', {})
            
            # Count matches for each requested code
            for code_type, code in all_codes:
                if code_type in codes and code in codes[code_type]:
                    match_count += 1
                    # Track non-RC matches for prioritization
                    if code_type != 'RC':
                        non_rc_matches += 1
            
            # Only include records with at least one match
            if match_count > 0:
                record['match_count'] = match_count
                record['non_rc_matches'] = non_rc_matches
                scored_results.append(record)
        
        # Sort by non-RC matches (highest first), then total matches, then cash price
        scored_results.sort(key=lambda x: (-x['non_rc_matches'], -x['match_count'], -x['cash_price']))
        
        return scored_results[:limit]
        
    except Exception as e:
        print(f"❌ Error querying database: {e}")
        return []

def format_results(results: List[Dict[str, Any]]) -> None:
    """Format and display search results."""
    if not results:
        print("No matching records found.")
        return
    
    print(f"🏥 Found {len(results)} matching operations:\n")
    
    for i, record in enumerate(results, 1):
        match_count = record.get('match_count', 0)
        non_rc_matches = record.get('non_rc_matches', 0)
        if non_rc_matches > 0:
            print(f"{i}. {record['description']} (🎯 {match_count} matches, {non_rc_matches} non-RC)")
        else:
            print(f"{i}. {record['description']} (🎯 {match_count} matches, RC only)")
        print(f"   🏥 {record['hospitals']['facility_name']} ({record['facility_id']})")
        print(f"   📍 {record['hospitals']['city']}, {record['hospitals']['state']}")
        
        # Format prices
        cash_price = f"${record['cash_price']:.2f}" if record['cash_price'] is not None else "N/A"
        gross_charge = f"${record['gross_charge']:.2f}" if record['gross_charge'] is not None else "N/A"
        print(f"   💰 Cash: {cash_price} | Gross: {gross_charge}")
        
        # Format negotiated prices
        if record['negotiated_min'] is not None and record['negotiated_max'] is not None:
            negotiated = f"${record['negotiated_min']:.2f} - ${record['negotiated_max']:.2f}"
        elif record['negotiated_min'] is not None:
            negotiated = f"${record['negotiated_min']:.2f} - N/A"
        elif record['negotiated_max'] is not None:
            negotiated = f"N/A - ${record['negotiated_max']:.2f}"
        else:
            negotiated = "N/A - N/A"
        print(f"   📊 Negotiated: {negotiated}")
        
        # Format codes
        codes_str = json.dumps(record['codes'], indent=2)
        print(f"   🏷️  Codes:\n{codes_str}")
        print(f"   📅 Ingested: {record['ingested_at']}")
        print()

def main():
    """Main function for command-line usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Find medical operations by standardized codes')
    parser.add_argument('--cpt', nargs='+', help='CPT codes to search for')
    parser.add_argument('--icd10', nargs='+', help='ICD-10 codes to search for')
    parser.add_argument('--hcpcs', nargs='+', help='HCPCS codes to search for')
    parser.add_argument('--rc', nargs='+', help='Revenue codes to search for')
    parser.add_argument('--facility-id', help='Filter by facility ID')
    parser.add_argument('--limit', type=int, default=100, help='Maximum number of results')
    parser.add_argument('--json', action='store_true', help='Output results as JSON')
    
    args = parser.parse_args()
    
    # Check if any codes were provided
    if not any([args.cpt, args.icd10, args.hcpcs, args.rc]):
        print("❌ Please provide at least one code type (--cpt, --icd10, --hcpcs, --rc)")
        parser.print_help()
        return
    
    # Search for records
    results = find_by_codes(
        cpt_codes=args.cpt,
        icd10_codes=args.icd10,
        hcpcs_codes=args.hcpcs,
        rc_codes=args.rc,
        facility_id=args.facility_id,
        limit=args.limit
    )
    
    if args.json:
        print(json.dumps(results, indent=2, default=str))
    else:
        format_results(results)

if __name__ == "__main__":
    main()
