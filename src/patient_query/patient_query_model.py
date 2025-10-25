"""
Patient Query Model - Generic interface for medical procedure queries.
Handles user queries about medical procedures and returns matching medical codes.
"""
import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
from pydantic import BaseModel, Field
from enum import Enum

logger = logging.getLogger(__name__)


class QueryResponse(BaseModel):
    """Response model for patient queries."""
    hcpcs_codes: List[str] = Field(default_factory=list, description="HCPCS procedure codes")
    rc_codes: List[str] = Field(default_factory=list, description="RC procedure codes")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence score (0-1)")
    reasoning: str = Field(default="", description="Explanation of the results")
    needs_clarification: bool = Field(default=False, description="Whether query needs clarification")
    clarification_message: Optional[str] = Field(default=None, description="Message asking for clarification")


class QueryStatus(str, Enum):
    """Status of the query processing."""
    SUCCESS = "success"
    NEEDS_CLARIFICATION = "needs_clarification"
    ERROR = "error"


class PatientQueryResult(BaseModel):
    """Complete result of a patient query."""
    status: QueryStatus
    response: Optional[QueryResponse] = None
    error_message: Optional[str] = None
    raw_model_output: Optional[Dict[str, Any]] = None


class ModelProvider(ABC):
    """Abstract base class for model providers."""
    
    @abstractmethod
    def query_model(self, user_query: str, system_prompt: str) -> Dict[str, Any]:
        """
        Query the model with user input and system prompt.
        
        Args:
            user_query: The user's medical procedure query
            system_prompt: System prompt for the model
            
        Returns:
            Raw model response as dictionary
        """
        pass


class PatientQueryModel:
    """
    Generic patient query model that can work with different AI providers.
    """
    
    def __init__(self, model_provider: ModelProvider):
        """
        Initialize the patient query model.
        
        Args:
            model_provider: The model provider implementation
        """
        self.model_provider = model_provider
        self.default_system_prompt = self._get_default_system_prompt()
    
    def _get_default_system_prompt(self) -> str:
        """Get the default system prompt for medical procedure queries."""
        return """You are a medical coding assistant that helps patients find relevant medical procedure codes based on their queries.

Your task is to:
1. Analyze the user's query about medical procedures
2. Identify relevant HCPCS (Healthcare Common Procedure Coding System) procedure (which includes any CPT or Current Procedural Terminology codes you find) codes and RC (Revenue Codes) procedure codes
3. Provide a confidence score (0-1) for the accuracy of your matches
4. Explain your reasoning

If the query is too vague or unclear, respond with needs_clarification: true and provide a helpful clarification message.

Return your response in JSON format with the following structure:
{
    "hcpcs_codes": ["list", "of", "hcpcs", "codes"],
    "rc_codes": ["list", "of", "rc", "codes"],
    "overall_confidence": 0.85,
    "reasoning": "Explanation of why these codes match the query",
    "needs_clarification": false,
    "clarification_message": null
}

Return as many relevant codes as you can. It is most important to include HCPCS codes. RC codes are lower priority. Focus on common medical procedures and provide accurate, relevant codes."""
    
    def process_query(self, user_query: str, system_prompt: Optional[str] = None) -> PatientQueryResult:
        """
        Process a patient query and return medical codes.
        
        Args:
            user_query: The user's medical procedure query
            system_prompt: Optional custom system prompt
            
        Returns:
            PatientQueryResult with codes and confidence
        """
        try:
            # Validate input
            if not user_query or not user_query.strip():
                return PatientQueryResult(
                    status=QueryStatus.ERROR,
                    error_message="Query cannot be empty"
                )
            
            # Use provided system prompt or default
            prompt = system_prompt or self.default_system_prompt
            
            # Query the model
            logger.info(f"Processing query: {user_query[:100]}...")
            raw_response = self.model_provider.query_model(user_query, prompt)
            
            # Parse and validate response
            response = self._parse_model_response(raw_response)
            
            # Check if clarification is needed
            if response.needs_clarification:
                return PatientQueryResult(
                    status=QueryStatus.NEEDS_CLARIFICATION,
                    response=response,
                    raw_model_output=raw_response
                )
            
            # Validate that we have some codes
            if not response.hcpcs_codes and not response.rc_codes:
                logger.warning("No medical codes found in response")
                return PatientQueryResult(
                    status=QueryStatus.NEEDS_CLARIFICATION,
                    response=QueryResponse(
                        needs_clarification=True,
                        clarification_message="I couldn't find specific medical codes for your query. Could you provide more details about the specific procedure or condition you're looking for?"
                    ),
                    raw_model_output=raw_response
                )
            
            return PatientQueryResult(
                status=QueryStatus.SUCCESS,
                response=response,
                raw_model_output=raw_response
            )
            
        except Exception as e:
            logger.error(f"Error processing query: {e}")
            return PatientQueryResult(
                status=QueryStatus.ERROR,
                error_message=str(e)
            )
    
    def _parse_model_response(self, raw_response: Dict[str, Any]) -> QueryResponse:
        """
        Parse the raw model response into a structured QueryResponse.
        
        Args:
            raw_response: Raw response from the model
            
        Returns:
            Parsed QueryResponse
        """
        try:
            # Extract fields with defaults
            hcpcs_codes = raw_response.get("hcpcs_codes", [])
            rc_codes = raw_response.get("rc_codes", [])
            confidence = raw_response.get("overall_confidence", 0.5)
            reasoning = raw_response.get("reasoning", "")
            needs_clarification = raw_response.get("needs_clarification", False)
            clarification_message = raw_response.get("clarification_message")
            
            # Ensure codes are lists of strings
            if not isinstance(hcpcs_codes, list):
                hcpcs_codes = []
            if not isinstance(rc_codes, list):
                rc_codes = []
            
            # Ensure confidence is a float between 0 and 1
            try:
                confidence = float(confidence)
                confidence = max(0.0, min(1.0, confidence))
            except (ValueError, TypeError):
                confidence = 0.5
            
            return QueryResponse(
                hcpcs_codes=hcpcs_codes,
                rc_codes=rc_codes,
                confidence=confidence,
                reasoning=reasoning,
                needs_clarification=needs_clarification,
                clarification_message=clarification_message
            )
            
        except Exception as e:
            logger.error(f"Error parsing model response: {e}")
            # Return a safe default response
            return QueryResponse(
                hcpcs_codes=[],
                rc_codes=[],
                confidence=0.0,
                reasoning="Error parsing model response",
                needs_clarification=True,
                clarification_message="There was an error processing your query. Please try again with more specific details."
            )
    
    def get_supported_procedures(self) -> List[str]:
        """
        Get a list of supported medical procedures (for documentation purposes).
        
        Returns:
            List of common medical procedures
        """
        return [
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
            "urinalysis",
            "physical examination"
        ]


# Example usage and testing
if __name__ == "__main__":
    # This would be used with an actual model provider
    # from claude import ClaudeProvider
    
    # model_provider = ClaudeProvider()
    query_model = PatientQueryModel(model_provider)
    
    result = query_model.process_query("where can I get a brain MRI scan without contrast")
    print(result)
    pass
