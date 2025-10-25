#!/usr/bin/env python3
"""
Interactive Patient Query CLI
A terminal-based interface for querying medical procedures using the patient query model.
"""
import os
import sys
import json
from pathlib import Path
from typing import Optional

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from patient_query import PatientQueryModel, ClaudeProvider, QueryStatus


class PatientQueryCLI:
    """Interactive CLI for patient queries."""
    
    def __init__(self):
        """Initialize the CLI."""
        self.query_model: Optional[PatientQueryModel] = None
        self.running = True
        
    def initialize(self) -> bool:
        """Initialize the patient query model."""
        try:
            print("🔧 Initializing Patient Query Model...")
            
            # Check for API key
            api_key = os.getenv('ANTHROPIC_API_KEY')
            if not api_key:
                print("❌ ANTHROPIC_API_KEY not found!")
                print("Please set your Claude API key in the .supabase.env file.")
                print("Example: ANTHROPIC_API_KEY=your-api-key-here")
                return False
            
            # Initialize Claude provider
            claude_provider = ClaudeProvider()
            
            # Test connection
            print("🔌 Testing Claude API connection...")
            if not claude_provider.test_connection():
                print("❌ Claude API connection failed!")
                return False
            
            # Initialize patient query model
            self.query_model = PatientQueryModel(claude_provider)
            
            print("✅ Patient Query Model initialized successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Failed to initialize: {e}")
            return False
    
    def display_welcome(self):
        """Display welcome message and instructions."""
        print("\n" + "="*60)
        print("🏥 PATIENT QUERY SYSTEM")
        print("="*60)
        print("Ask me about medical procedures and I'll find relevant codes!")
        print("\nExamples:")
        print("  • 'where can I get knee surgery'")
        print("  • 'I need a hip replacement'")
        print("  • 'cardiac catheterization near me'")
        print("  • 'colonoscopy procedure'")
        print("\nCommands:")
        print("  • Type 'help' for more examples")
        print("  • Type 'quit' or 'exit' to stop")
        print("  • Type 'clear' to clear the screen")
        print("="*60)
    
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
    
    def format_response(self, result) -> str:
        """Format the query result for display."""
        if not result.response:
            return f"❌ Error: {result.error_message or 'Unknown error'}"
        
        response = result.response
        output = []
        
        # Status indicator
        if result.status == QueryStatus.SUCCESS:
            output.append("✅ SUCCESS")
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
    
    def process_query(self, user_input: str):
        """Process a user query."""
        if not self.query_model:
            print("❌ Patient Query Model not initialized!")
            return
        
        print(f"\n🔍 Processing: '{user_input}'")
        print("-" * 50)
        
        try:
            result = self.query_model.process_query(user_input)
            formatted_output = self.format_response(result)
            print(formatted_output)
            
        except Exception as e:
            print(f"❌ Error processing query: {e}")
    
    def handle_command(self, user_input: str) -> bool:
        """Handle special commands. Returns True if command was handled."""
        command = user_input.lower().strip()
        
        if command in ['quit', 'exit', 'q']:
            print("\n👋 Goodbye! Thanks for using the Patient Query System!")
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
            else:
                print("❌ Patient Query Model is not initialized")
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
    cli = PatientQueryCLI()
    cli.run()


if __name__ == "__main__":
    main()
