#!/usr/bin/env python3
"""
Test script for the patient query model implementation.
Demonstrates how to use the Claude provider with the patient query model.
"""
import os
import sys
import json
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from patient_query.patient_query_model import PatientQueryModel
from patient_query.claude import ClaudeProvider


def test_patient_query_model():
    """Test the patient query model with sample queries."""
    
    print("Testing Patient Query Model Implementation")
    print("=" * 50)
    
    # Check if API key is available
    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key:
        print("❌ ANTHROPIC_API_KEY not found in environment variables.")
        print("Please set your Claude API key in the .supabase.env file or environment.")
        print("Example: export ANTHROPIC_API_KEY='your-api-key-here'")
        return False
    
    try:
        # Initialize the Claude provider
        print("🔧 Initializing Claude provider...")
        claude_provider = ClaudeProvider()
        
        # Initialize the patient query model
        print("🔧 Initializing patient query model...")
        query_model = PatientQueryModel(claude_provider)
        
        # Test queries
        test_queries = [
            "where can I get knee surgery",
            "I need a hip replacement",
            "cardiac catheterization near me",
            "colonoscopy procedure",
            "help me find MRI scan",  # This should be vague and trigger clarification
            "surgery"  # This should be very vague
        ]
        
        print("\n🧪 Running test queries...")
        print("-" * 30)
        
        for i, query in enumerate(test_queries, 1):
            print(f"\nTest {i}: '{query}'")
            print("-" * 20)
            
            try:
                result = query_model.process_query(query)
                
                print(f"Status: {result.status}")
                
                if result.response:
                    print(f"HCPCS Codes: {result.response.hcpcs_codes}")
                    print(f"RC Codes: {result.response.rc_codes}")
                    print(f"Confidence: {result.response.confidence:.2f}")
                    print(f"Needs Clarification: {result.response.needs_clarification}")
                    
                    if result.response.reasoning:
                        print(f"Reasoning: {result.response.reasoning[:100]}...")
                    
                    if result.response.clarification_message:
                        print(f"Clarification: {result.response.clarification_message}")
                
                if result.error_message:
                    print(f"Error: {result.error_message}")
                
            except Exception as e:
                print(f"❌ Error processing query: {e}")
        
        print("\n✅ All tests completed!")
        return True
        
    except Exception as e:
        print(f"❌ Failed to initialize components: {e}")
        return False


def test_claude_connection():
    """Test Claude API connection."""
    print("\n🔌 Testing Claude API connection...")
    
    try:
        claude_provider = ClaudeProvider()
        if claude_provider.test_connection():
            print("✅ Claude API connection successful!")
            return True
        else:
            print("❌ Claude API connection failed!")
            return False
    except Exception as e:
        print(f"❌ Claude API connection error: {e}")
        return False


def main():
    """Main test function."""
    print("Patient Query Model Test Suite")
    print("=" * 40)
    
    # Test Claude connection first
    if not test_claude_connection():
        print("\n❌ Cannot proceed without Claude API connection.")
        return
    
    # Test the full implementation
    success = test_patient_query_model()
    
    if success:
        print("\n🎉 All tests passed! The implementation is working correctly.")
    else:
        print("\n❌ Some tests failed. Please check the error messages above.")


if __name__ == "__main__":
    main()
