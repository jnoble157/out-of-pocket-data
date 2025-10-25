#!/usr/bin/env python3
"""
Combined Patient Query and Supabase Search CLI
Combines patient query functionality with Supabase database search.
Flow: User Query → Claude API → Medical Codes → Supabase Search → Results
"""
import os
import sys
import json
from pathlib import Path
from typing import Optional, List, Dict, Any

# Add the parent directory to the path so we can import from src
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from patient_query.patient_query_model import PatientQueryModel, QueryStatus
from patient_query.claude import ClaudeProvider
from database import supabase_manager
from query_cache import QueryCacheManager
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.supabase.env')


class CombinedPatientQueryCLI:
    """Combined CLI for patient queries with Supabase integration."""
    
    def __init__(self):
        """Initialize the combined CLI."""
        self.query_model: Optional[PatientQueryModel] = None
        self.cache_manager: Optional[QueryCacheManager] = None
        self.running = True
        
    def initialize(self) -> bool:
        """Initialize both the patient query model and Supabase."""
        try:
            print("🔧 Initializing Combined Patient Query System...")
            
            # Check for API keys
            api_key = os.getenv('ANTHROPIC_API_KEY')
            if not api_key:
                print("❌ ANTHROPIC_API_KEY not found!")
                print("Please set your Claude API key in the .supabase.env file.")
                return False
            
            # Initialize Claude provider
            claude_provider = ClaudeProvider()
            
            # Test Claude connection
            print("🔌 Testing Claude API connection...")
            if not claude_provider.test_connection():
                print("❌ Claude API connection failed!")
                return False
            
            # Initialize patient query model
            self.query_model = PatientQueryModel(claude_provider)
            
            # Initialize Supabase
            print("🗄️  Initializing Supabase connection...")
            supabase_manager.initialize()
            
            # Initialize query cache manager
            print("🧠 Initializing query cache system...")
            self.cache_manager = QueryCacheManager()
            
            print("✅ Combined system initialized successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Failed to initialize: {e}")
            return False
    
    def display_welcome(self):
        """Display welcome message and instructions."""
        print("\n" + "="*70)
        print("🏥 COMBINED PATIENT QUERY & DATABASE SEARCH SYSTEM")
        print("="*70)
        print("Ask me about medical procedures and I'll:")
        print("  1. 🔍 Find relevant medical codes using AI")
        print("  2. 🗄️  Search our database for matching procedures")
        print("  3. 💰 Show you pricing and facility information")
        print("\nExamples:")
        print("  • 'where can I get knee surgery'")
        print("  • 'I need a hip replacement'")
        print("  • 'cardiac catheterization near me'")
        print("  • 'colonoscopy procedure'")
        print("\nCommands:")
        print("  • Type 'help' for more examples")
        print("  • Type 'quit' or 'exit' to stop")
        print("  • Type 'clear' to clear the screen")
        print("  • Type 'status' to check system status")
        print("  • Type 'cache-stats' to view cache statistics")
        print("  • Type 'clear-cache' to clear the query cache")
        print("="*70)
    
    def display_help(self):
        """Display help information."""
        print("\n📚 HELP - Medical Procedure Query Examples")
        print("-" * 50)
        
        examples = [
            "knee surgery",
            "hip replacement", 
            "cardiac catheterization",
            "colonoscopy",
            "appendectomy",
            "gallbladder removal",
            "hernia repair",
            "cataract surgery",
            "mammography",
            "MRI scan",
            "CT scan",
            "X-ray",
            "blood test",
            "physical examination",
            "emergency room visit"
        ]
        
        print("Try asking about these procedures:")
        for i, example in enumerate(examples, 1):
            print(f"  {i:2d}. {example}")
        
        print("\n💡 Tips:")
        print("  • Be specific about the procedure you need")
        print("  • Include location if relevant (e.g., 'near me')")
        print("  • Ask about symptoms to get diagnosis codes")
        print("  • Mention insurance type if relevant")
        print("-" * 50)
    
    def search_supabase_by_codes(self, hspcs_codes: List[str], rc_codes: List[str], limit: int = 20) -> List[Dict[str, Any]]:
        """
        Search Supabase database using the codes returned by Claude.
        
        Args:
            hspcs_codes: HSPCS codes from Claude
            rc_codes: RC codes from Claude  
            limit: Maximum number of results
            
        Returns:
            List of matching medical operations
        """
        try:
            print(f"🗄️  Searching database with {len(hspcs_codes)} HSPCS and {len(rc_codes)} RC codes...")
            
            if not hspcs_codes and not rc_codes:
                print("⚠️  No codes provided for database search")
                return []
            
            # Build query to get all records that match any of the codes
            query = supabase_manager.client.table('medical_operations').select(
                'facility_id, description, cash_price, gross_charge, negotiated_min, negotiated_max, rc_code, hcpcs_code, currency, ingested_at, hospitals!inner(facility_name, city, state)'
            )
            
            # Build OR conditions for code matching
            # Use Supabase's or_() method with proper syntax
            if hspcs_codes or rc_codes:
                all_filters = []
                
                # Add HCPCS code filters
                for code in hspcs_codes:
                    all_filters.append(f'hcpcs_code.eq.{code}')
                
                # Add RC code filters
                for code in rc_codes:
                    all_filters.append(f'rc_code.eq.{code}')
                
                # Apply OR filter (Supabase expects a comma-separated string)
                if all_filters:
                    or_condition = ','.join(all_filters)
                    query = query.or_(or_condition)
            
            # Execute query
            result = query.execute()
            
            if not result.data:
                print("❌ No data found in database")
                return []
            
            # Calculate match scores and filter
            scored_results = []
            for record in result.data:
                match_count = 0
                non_rc_matches = 0
                
                # Check HCPCS matches
                if hspcs_codes and record.get('hcpcs_code'):
                    if record['hcpcs_code'] in hspcs_codes:
                        match_count += 1
                        non_rc_matches += 1
                
                # Check RC matches
                if rc_codes and record.get('rc_code'):
                    if record['rc_code'] in rc_codes:
                        match_count += 1
                        # RC matches don't count toward non_rc_matches
                
                # Only include records with at least one match
                if match_count > 0:
                    record['match_count'] = match_count
                    record['non_rc_matches'] = non_rc_matches
                    scored_results.append(record)
            
            # Sort by non-RC matches (highest first), then total matches, then cash price
            scored_results.sort(key=lambda x: (-x['non_rc_matches'], -x['match_count'], -x['cash_price'] if x['cash_price'] else 0))
            
            return scored_results[:limit]

            
        except Exception as e:
            print(f"❌ Error querying database: {e}")
            return []
    
    def format_codes_response(self, result) -> str:
        """Format the Claude codes response for display."""
        if not result.response:
            return f"❌ Error: {result.error_message or 'Unknown error'}"
        
        response = result.response
        output = []
        
        # Status indicator
        if result.status == QueryStatus.SUCCESS:
            output.append("✅ CODES FOUND")
        elif result.status == QueryStatus.NEEDS_CLARIFICATION:
            output.append("❓ NEEDS CLARIFICATION")
        else:
            output.append("❌ ERROR")
        
        output.append("")
        
        # Medical codes
        if response.hspcs_codes:
            output.append("🏥 HSPCS Procedure Codes:")
            for code in response.hspcs_codes:
                output.append(f"   • {code}")
            output.append("")
        
        if response.rc_codes:
            output.append("🔧 RC Procedure Codes:")
            for code in response.rc_codes:
                output.append(f"   • {code}")
            output.append("")
        
        # Confidence score
        confidence_emoji = "🟢" if response.confidence >= 0.8 else "🟡" if response.confidence >= 0.6 else "🔴"
        output.append(f"{confidence_emoji} Confidence Score: {response.confidence:.1%}")
        output.append("")
        
        # Reasoning
        if response.reasoning:
            output.append("💭 Reasoning:")
            # Wrap long text
            words = response.reasoning.split()
            lines = []
            current_line = ""
            for word in words:
                if len(current_line + word) > 70:
                    if current_line:
                        lines.append(current_line.strip())
                    current_line = word
                else:
                    current_line += " " + word if current_line else word
            if current_line:
                lines.append(current_line.strip())
            
            for line in lines:
                output.append(f"   {line}")
            output.append("")
        
        # Clarification message
        if response.needs_clarification and response.clarification_message:
            output.append("❓ Clarification Needed:")
            output.append(f"   {response.clarification_message}")
            output.append("")
        
        return "\n".join(output)
    
    def format_database_results(self, results: List[Dict[str, Any]]) -> str:
        """Format database search results for display."""
        if not results:
            return "❌ No matching procedures found in database."
        
        output = []
        output.append(f"🗄️  DATABASE RESULTS ({len(results)} procedures found):")
        output.append("=" * 50)
        
        for i, record in enumerate(results, 1):
            match_count = record.get('match_count', 0)
            non_rc_matches = record.get('non_rc_matches', 0)
            
            if non_rc_matches > 0:
                output.append(f"{i}. {record['description']} (🎯 {match_count} matches, {non_rc_matches} non-RC)")
            else:
                output.append(f"{i}. {record['description']} (🎯 {match_count} matches, RC only)")
            
            output.append(f"   🏥 {record['hospitals']['facility_name']} ({record['facility_id']})")
            output.append(f"   📍 {record['hospitals']['city']}, {record['hospitals']['state']}")
            
            # Format prices
            cash_price = f"${record['cash_price']:.2f}" if record['cash_price'] is not None else "N/A"
            gross_charge = f"${record['gross_charge']:.2f}" if record['gross_charge'] is not None else "N/A"
            output.append(f"   💰 Cash: {cash_price} | Gross: {gross_charge}")
            
            # Format negotiated prices
            if record['negotiated_min'] is not None and record['negotiated_max'] is not None:
                negotiated = f"${record['negotiated_min']:.2f} - ${record['negotiated_max']:.2f}"
            elif record['negotiated_min'] is not None:
                negotiated = f"${record['negotiated_min']:.2f} - N/A"
            elif record['negotiated_max'] is not None:
                negotiated = f"N/A - ${record['negotiated_max']:.2f}"
            else:
                negotiated = "N/A - N/A"
            output.append(f"   📊 Negotiated: {negotiated}")
            
            # Show relevant codes
            codes_display = {}
            if record.get('hcpcs_code'):
                codes_display['HCPCS'] = record['hcpcs_code']
            if record.get('rc_code'):
                codes_display['RC'] = record['rc_code']
            
            if codes_display:
                codes_str = json.dumps(codes_display, indent=2)
                output.append(f"   🏷️  Codes:\n{codes_str}")
            
            output.append(f"   📅 Ingested: {record['ingested_at']}")
            output.append("")
        
        return "\n".join(output)
    
    def process_query(self, user_input: str):
        """Process a user query through the complete pipeline."""
        if not self.query_model:
            print("❌ Patient Query Model not initialized!")
            return
        
        print(f"\n🔍 Processing: '{user_input}'")
        print("-" * 50)
        
        try:
            # Step 1: Check cache first
            cached_result = None
            if self.cache_manager:
                print("🧠 Step 1: Checking cache for similar queries...")
                cached_result = self.cache_manager.check_cache(user_input)
                
                if cached_result:
                    print(f"✅ Cache hit found! (Similarity: {cached_result.similarity_score:.1%})")
                    print(f"📋 Using cached codes from: '{cached_result.original_query}'")
                    
                    # Use cached codes
                    hspcs_codes = cached_result.hspcs_codes
                    rc_codes = cached_result.rc_codes
                    reasoning = cached_result.reasoning
                    confidence = cached_result.confidence_score
                    
                    # Display cached codes
                    print("\n🏥 Cached Medical Codes:")
                    if hspcs_codes:
                        print("   HSPCS Procedure Codes:")
                        for code in hspcs_codes:
                            print(f"   • {code}")
                    if rc_codes:
                        print("   RC Procedure Codes:")
                        for code in rc_codes:
                            print(f"   • {code}")
                    
                    print(f"\n💭 Cached Reasoning: {reasoning}")
                    print(f"🎯 Cached Confidence: {confidence:.1%}")
                    
                else:
                    print("❌ No cache hit found, proceeding with Claude API...")
            
            # Step 2: If no cache hit, get codes from Claude
            if not cached_result:
                print("\n🤖 Step 2: Getting medical codes from Claude...")
                result = self.query_model.process_query(user_input)
                
                # Display codes
                codes_output = self.format_codes_response(result)
                print(codes_output)
                
                # Check if we got codes to search with
                if result.status != QueryStatus.SUCCESS or not result.response:
                    print("⚠️  Cannot search database without valid codes.")
                    return
                
                response = result.response
                if not response.hspcs_codes and not response.rc_codes:
                    print("⚠️  No codes found to search database with.")
                    return
                
                # Use fresh codes from Claude
                hspcs_codes = response.hspcs_codes
                rc_codes = response.rc_codes
                reasoning = response.reasoning
                confidence = response.confidence
                
                # Store in cache if confidence is high enough
                if self.cache_manager and confidence >= 0.90:
                    print("💾 Storing high-confidence result in cache...")
                    self.cache_manager.store_cache(
                        query=user_input,
                        hspcs_codes=hspcs_codes,
                        rc_codes=rc_codes,
                        reasoning=reasoning,
                        confidence=confidence
                    )
            
            # Step 3: Search Supabase with the codes (cached or fresh)
            print("\n🗄️  Step 3: Searching database with codes...")
            db_results = self.search_supabase_by_codes(
                hspcs_codes=hspcs_codes,
                rc_codes=rc_codes,
                limit=20
            )
            
            # Display database results
            db_output = self.format_database_results(db_results)
            print(db_output)
            
        except Exception as e:
            print(f"❌ Error processing query: {e}")
    
    def handle_command(self, user_input: str) -> bool:
        """Handle special commands. Returns True if command was handled."""
        command = user_input.lower().strip()
        
        if command in ['quit', 'exit', 'q']:
            print("\n👋 Goodbye! Thanks for using the Combined Patient Query System!")
            self.running = False
            return True
        
        elif command == 'help':
            self.display_help()
            return True
        
        elif command == 'clear':
            os.system('clear' if os.name == 'posix' else 'cls')
            return True
        
        elif command == 'status':
            if self.query_model:
                print("✅ Patient Query Model is running")
                print("🔌 Claude API connection is active")
                print("🗄️  Supabase database is connected")
                
                # Show cache status
                if self.cache_manager:
                    cache_stats = self.cache_manager.get_cache_stats()
                    print(f"🧠 Query cache: {'enabled' if cache_stats['cache_enabled'] else 'disabled'}")
                    print(f"🔗 Embeddings: {'available' if cache_stats.get('embeddings_available', False) else 'unavailable'}")
                    print(f"📊 Cached queries: {cache_stats['total_queries']}")
                    print(f"🎯 High-confidence queries: {cache_stats['high_confidence_queries']}")
                    print(f"⚙️  Similarity threshold: {cache_stats['similarity_threshold']}")
                else:
                    print("❌ Query cache not initialized")
            else:
                print("❌ System is not initialized")
            return True
        
        elif command == 'cache-stats':
            if self.cache_manager:
                cache_stats = self.cache_manager.get_cache_stats()
                print("\n📊 CACHE STATISTICS")
                print("=" * 30)
                print(f"Cache enabled: {cache_stats['cache_enabled']}")
                print(f"Embeddings available: {cache_stats.get('embeddings_available', False)}")
                print(f"Total cached queries: {cache_stats['total_queries']}")
                print(f"High-confidence queries: {cache_stats['high_confidence_queries']}")
                print(f"Similarity threshold: {cache_stats['similarity_threshold']}")
                print(f"Min confidence to cache: {cache_stats['min_confidence_to_cache']}")
                if 'error' in cache_stats:
                    print(f"Error: {cache_stats['error']}")
            else:
                print("❌ Cache manager not initialized")
            return True
        
        elif command == 'clear-cache':
            if self.cache_manager:
                print("🗑️  Clearing query cache...")
                if self.cache_manager.clear_cache():
                    print("✅ Cache cleared successfully")
                else:
                    print("❌ Failed to clear cache")
            else:
                print("❌ Cache manager not initialized")
            return True
        
        return False
    
    def run(self):
        """Main CLI loop."""
        # Initialize the system
        if not self.initialize():
            return
        
        # Display welcome message
        self.display_welcome()
        
        # Main interaction loop
        while self.running:
            try:
                # Get user input
                user_input = input("\n🏥 Enter your medical procedure query: ").strip()
                
                # Handle empty input
                if not user_input:
                    print("Please enter a medical procedure query or command.")
                    continue
                
                # Handle commands
                if self.handle_command(user_input):
                    continue
                
                # Process the query
                self.process_query(user_input)
                
            except KeyboardInterrupt:
                print("\n\n👋 Interrupted by user. Goodbye!")
                break
            except EOFError:
                print("\n\n👋 End of input. Goodbye!")
                break
            except Exception as e:
                print(f"\n❌ Unexpected error: {e}")


def main():
    """Main entry point."""
    cli = CombinedPatientQueryCLI()
    cli.run()


if __name__ == "__main__":
    main()
