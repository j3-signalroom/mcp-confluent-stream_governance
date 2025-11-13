from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
import json
import time
from mcp.types import CallToolResult

from client_manager import ClientManager
from helpers import get_ensured_param 
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class ReadFlinkStatementArguments(BaseModel):
    """Arguments for reading a Flink statement"""
    base_url: HttpUrl | None = Field(
        default=None,
        description="The base URL of the Flink REST API."
    )
    organization_id: str | None = Field(
        default=None,
        description="The unique identifier for the organization."
    )
    environment_id: str | None = Field(
        default=None,
        description="The unique identifier for the environment."
    )
    statement_name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        pattern=r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$",
        description="The user provided name of the resource, unique within this environment."
    )
    timeout_in_milliseconds: int = Field(
        default=60000,
        description=(
            "The function implements pagination. It will continue to fetch results using the "
            "next page token until either there are no more results or the timeout is reached. "
            "Tables backed by kafka topics can be thought of as never-ending streams as data could "
            "be continuously produced in near real-time. Therefore, if you wish to sample values "
            "from a stream, you may want to set a timeout. If you are reading a statement after "
            "creating it, you may need to retry a couple times to ensure that the statement is "
            "ready and receiving data."
        )
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("FLINK_REST_ENDPOINT")
        return v
    
    @field_validator('organization_id', 'environment_id', 'statement_name', mode='before')
    @classmethod
    def strip_whitespace(cls, v: str | None) -> str | None:
        """Strip whitespace from string fields"""
        if v is not None and isinstance(v, str):
            return v.strip()
        return v


class ReadFlinkStatementHandler(BaseToolHandler):
    """Handler for reading Flink SQL statement results with pagination"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Make a request to read a statement and its results.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Arguments containing statement identification and timeout
            session_id: Optional session identifier
            
        Returns:
            Result containing all statement results (paginated)
        """
        if tool_arguments is None:
            return self.create_response(
                "No arguments provided for reading Flink statement",
                is_error=True
            )
        
        try:
            # Parse and validate arguments
            args = ReadFlinkStatementArguments.model_validate(tool_arguments)
        except Exception as e:
            return self.create_response(
                f"Invalid arguments: {str(e)}",
                is_error=True
            )
        
        # Get ensured parameters with fallback to env vars
        try:
            organization_id = get_ensured_param(
                "FLINK_ORG_ID",
                "Organization ID is required",
                args.organization_id
            )
            environment_id = get_ensured_param(
                "FLINK_ENV_ID",
                "Environment ID is required",
                args.environment_id
            )
        except ValueError as e:
            return self.create_response(
                str(e),
                is_error=True
            )
        
        # Update base URL if provided
        if args.base_url is not None and str(args.base_url) != "":
            client_manager.set_confluent_cloud_flink_endpoint(str(args.base_url))
        
        # Get the Flink REST client
        client = client_manager.get_confluent_cloud_flink_rest_client()
        
        # Initialize pagination variables
        all_results: list[Any] = []
        next_token: str | None = None
        
        # Calculate timeout
        # If timeout is -1 or undefined behavior, set to None (no timeout)
        timeout_ms = args.timeout_in_milliseconds
        timeout_end = None if timeout_ms == -1 else (time.time() * 1000 + timeout_ms)
        
        def has_timed_out() -> bool:
            """Check if timeout period has elapsed"""
            if timeout_end is None:
                return False
            return time.time() * 1000 >= timeout_end
        
        try:
            # Pagination loop
            while True:
                # Build URL
                url = (
                    f"/sql/v1/organizations/{organization_id}"
                    f"/environments/{environment_id}/statements/{args.statement_name}/results"
                )
                
                # Build query parameters - only include page_token if it's defined
                params = {}
                if next_token:
                    params["page_token"] = next_token
                
                # Make the GET API call
                response = await client.get(
                    url,
                    params=params if params else None
                )
                
                if response.status_code >= 400:
                    error_text = await response.text()
                    return self.create_response(
                        f"Failed to read Flink SQL statement: {error_text}",
                        is_error=True
                    )
                
                # Parse response
                result = await response.json()
                
                # Accumulate results
                results_data = result.get("results", {}).get("data", [])
                all_results.extend(results_data)
                
                # Extract next token from metadata
                metadata_next = result.get("metadata", {}).get("next")
                if metadata_next:
                    # Parse page_token from URL query string
                    # Example: "https://...?page_token=abc123"
                    parts = metadata_next.split("page_token=")
                    next_token = parts[1] if len(parts) > 1 else None
                else:
                    next_token = None
                
                # Check if we should continue
                if not next_token or has_timed_out():
                    break
            
            return self.create_response(
                f"Flink SQL Statement Results: {json.dumps(all_results, indent=2)}"
            )
            
        except Exception as e:
            return self.create_response(
                f"Error reading Flink SQL statement: {str(e)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.READ_FLINK_STATEMENT,
            description="Make a request to read a statement and its results",
            inputSchema=ReadFlinkStatementArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["FLINK_API_KEY", "FLINK_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True