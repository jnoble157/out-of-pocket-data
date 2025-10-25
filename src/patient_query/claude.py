"""
Claude API integration for medical procedure queries.
Implements the ModelProvider interface for Claude API calls.
"""
import os
import json
import logging
from typing import Dict, Any, Optional
from anthropic import Anthropic
from dotenv import load_dotenv

from .patient_query_model import ModelProvider

# Load environment variables
load_dotenv('.supabase.env')

logger = logging.getLogger(__name__)


class ClaudeProvider(ModelProvider):
    """
    Claude API provider implementation for medical procedure queries.
    """
    
    def __init__(self, api_key: Optional[str] = None, model: str = "claude-sonnet-4-5-20250929"):
        """
        Initialize Claude provider.
        
        Args:
            api_key: Claude API key (if not provided, will use ANTHROPIC_API_KEY env var)
            model: Claude model to use
        """
        self.api_key = api_key or os.getenv('ANTHROPIC_API_KEY')
        self.model = model
        
        if not self.api_key:
            raise ValueError("Claude API key not provided. Set ANTHROPIC_API_KEY environment variable or pass api_key parameter.")
        
        try:
            self.client = Anthropic(api_key=self.api_key)
            logger.info(f"Claude provider initialized with model: {self.model}")
        except Exception as e:
            logger.error(f"Failed to initialize Claude client: {e}")
            raise
    
    def query_model(self, user_query: str, system_prompt: str) -> Dict[str, Any]:
        """
        Query Claude with user input and system prompt.
        
        Args:
            user_query: The user's medical procedure query
            system_prompt: System prompt for the model
            
        Returns:
            Raw model response as dictionary
        """
        try:
            logger.info(f"Querying Claude with query: {user_query[:100]}...")
            
            # Create the message for Claude
            message = self.client.messages.create(
                model=self.model,
                max_tokens=1024,
                system=system_prompt,
                messages=[
                    {
                        "role": "user",
                        "content": user_query
                    }
                ]
            )
            
            # Extract response text
            response_text = message.content[0].text
            logger.debug(f"Raw Claude response: {response_text}")
            
            # Parse JSON response
            result = self._parse_claude_response(response_text)
            
            logger.info(f"Successfully parsed Claude response with {len(result.get('hcpcs_codes', []))} HCPCS codes and {len(result.get('rc_codes', []))} RC codes")
            
            return result
            
        except Exception as e:
            logger.error(f"Error querying Claude: {e}")
            raise
    
    def _parse_claude_response(self, response_text: str) -> Dict[str, Any]:
        """
        Parse Claude's response text into a structured dictionary.
        
        Args:
            response_text: Raw text response from Claude
            
        Returns:
            Parsed response dictionary
        """
        try:
            # Clean up the response text
            cleaned_text = self._clean_json_response(response_text)
            
            # Try to parse as JSON
            result = json.loads(cleaned_text)
            
            # Validate the structure
            return self._validate_response_structure(result)
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Claude response as JSON: {e}")
            logger.error(f"Response text: {response_text}")
            
            # Return a fallback response
            return self._create_fallback_response(response_text)
        
        except Exception as e:
            logger.error(f"Error parsing Claude response: {e}")
            return self._create_fallback_response(response_text)
    
    def _clean_json_response(self, response_text: str) -> str:
        """
        Clean up Claude's response to extract JSON.
        
        Args:
            response_text: Raw response text
            
        Returns:
            Cleaned JSON string
        """
        # Remove markdown code blocks if present
        if "```json" in response_text:
            # Extract content between ```json and ```
            start = response_text.find("```json") + 7
            end = response_text.find("```", start)
            if end != -1:
                response_text = response_text[start:end].strip()
        elif "```" in response_text:
            # Extract content between ``` and ```
            start = response_text.find("```") + 3
            end = response_text.find("```", start)
            if end != -1:
                response_text = response_text[start:end].strip()
        
        # Remove any leading/trailing whitespace
        response_text = response_text.strip()
        
        return response_text
    
    def _validate_response_structure(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate and normalize the response structure.
        
        Args:
            result: Parsed JSON result
            
        Returns:
            Validated and normalized result
        """
        # Ensure required fields exist with defaults
        validated_result = {
            "hcpcs_codes": result.get("hcpcs_codes", []),
            "rc_codes": result.get("rc_codes", []),
            "overall_confidence": result.get("overall_confidence", 0.5),
            "reasoning": result.get("reasoning", ""),
            "needs_clarification": result.get("needs_clarification", False),
            "clarification_message": result.get("clarification_message")
        }
        
        # Ensure codes are lists
        if not isinstance(validated_result["hcpcs_codes"], list):
            validated_result["hcpcs_codes"] = []
        if not isinstance(validated_result["rc_codes"], list):
            validated_result["rc_codes"] = []
        
        # Ensure confidence is a float between 0 and 1
        try:
            confidence = float(validated_result["overall_confidence"])
            validated_result["overall_confidence"] = max(0.0, min(1.0, confidence))
        except (ValueError, TypeError):
            validated_result["overall_confidence"] = 0.5
        
        return validated_result
    
    def _create_fallback_response(self, response_text: str) -> Dict[str, Any]:
        """
        Create a fallback response when JSON parsing fails.
        
        Args:
            response_text: Raw response text
            
        Returns:
            Fallback response dictionary
        """
        logger.warning("Creating fallback response due to parsing failure")
        
        # Try to extract some information from the text
        hcpcs_codes = []
        rc_codes = []
        
        # Look for common patterns in the response
        lines = response_text.split('\n')
        for line in lines:
            line = line.strip()
            # Look for medical codes (format: alphanumeric codes)
            if any(char.isalpha() and char.isupper() for char in line[:2]):
                if any(char.isdigit() for char in line):
                    # This might be a medical code
                    if len(line) >= 3:
                        if line[0].isupper() and line[1:3].isdigit():
                            # Could be any type of medical code
                            hcpcs_codes.append(line[:3])
        
        return {
            "hcpcs_codes": hcpcs_codes,
            "rc_codes": rc_codes,
            "overall_confidence": 0.3,  # Low confidence due to parsing failure
            "reasoning": f"Could not parse Claude's response properly. Raw response: {response_text[:200]}...",
            "needs_clarification": True,
            "clarification_message": "I had trouble understanding your request. Could you provide more specific details about the medical procedure you're looking for?"
        }
    
    def test_connection(self) -> bool:
        """
        Test the Claude API connection.
        
        Returns:
            True if connection is successful, False otherwise
        """
        try:
            # Send a simple test query
            test_response = self.query_model(
                "test query",
                "You are a helpful assistant. Respond with a simple JSON: {\"test\": \"success\"}"
            )
            
            logger.info("Claude API connection test successful")
            return True
            
        except Exception as e:
            logger.error(f"Claude API connection test failed: {e}")
            return False


# Example usage
if __name__ == "__main__":
    # Test the Claude provider
    try:
        provider = ClaudeProvider()
        
        # Test connection
        if provider.test_connection():
            print("Claude API connection successful!")
            
            # Test a sample query
            result = provider.query_model(
                "where can I get knee surgery",
                "You are a medical coding assistant. Return JSON with icd10_codes, cpt_codes, overall_confidence, and reasoning."
            )
            
            print("Sample query result:")
            print(json.dumps(result, indent=2))
        else:
            print("Claude API connection failed!")
            
    except Exception as e:
        print(f"Error testing Claude provider: {e}")
