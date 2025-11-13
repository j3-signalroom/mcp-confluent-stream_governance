from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
import json
from mcp.types import CallToolResult

from client_manager import ClientManager
from helpers import get_ensured_param
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class ListFlinkStatementsArguments(BaseModel):
    """Arguments for listing Flink statements"""
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
    compute_pool_id: str | None = Field(
        default=None,
        description="Filter the results by exact match for compute_pool."
    )
    page_size: int = Field(
        default=10,
        ge=0,
        le=100,
        description="A pagination size for collection requests."
    )
    page_token: str | None = Field(
        default=None,
        max_length=255,
        description="An opaque pagination token for collection requests."
    )
    label_selector: str | None = Field(
        default=None,
        description="A comma-separated label selector to filter the statements."
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("FLINK_REST_ENDPOINT")
        return v
    
    @field_validator('compute_pool_id', mode='before')
    @classmethod
    def set_default_compute_pool_id(cls, v: str | None) -> str | None:
        """Set default compute_pool_id from environment if not provided"""
        if v is None or v == "":
            return os.getenv("FLINK_COMPUTE_POOL_ID")
        return v


class ListFlinkStatementsHandler(BaseToolHandler):
    """Handler for listing Flink SQL statements in Confluent Cloud"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Retrieve a sorted, filtered, paginated list of all statements.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Optional arguments for filtering and pagination
            session_id: Optional session identifier
            
        Returns:
            Result containing the list of statements
        """
        try:
            # Parse and validate arguments
            args = ListFlinkStatementsArguments.model_validate(tool_arguments or {})
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
        
        # Build query parameters - only include non-None values
        query_params: dict[str, Any] = {
            "page_size": args.page_size,
        }
        
        if args.compute_pool_id:
            query_params["spec.compute_pool_id"] = args.compute_pool_id
        if args.page_token:
            query_params["page_token"] = args.page_token
        if args.label_selector:
            query_params["label_selector"] = args.label_selector
        
        try:
            # Make the GET API call with path and query parameters
            url = (
                f"/sql/v1/organizations/{organization_id}"
                f"/environments/{environment_id}/statements"
            )
            
            response = await client.get(
                url,
                params=query_params
            )
            
            if response.status_code >= 400:
                error_text = await response.text()
                return self.create_response(
                    f"Failed to list Flink SQL statements: {error_text}",
                    is_error=True
                )
            
            result = await response.json()
            return self.create_response(
                json.dumps(result, indent=2)
            )
            
        except Exception as e:
            return self.create_response(
                f"Error listing Flink SQL statements: {str(e)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.LIST_FLINK_STATEMENTS,
            description="Retrieve a sorted, filtered, paginated list of all statements.",
            inputSchema=ListFlinkStatementsArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["FLINK_API_KEY", "FLINK_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True