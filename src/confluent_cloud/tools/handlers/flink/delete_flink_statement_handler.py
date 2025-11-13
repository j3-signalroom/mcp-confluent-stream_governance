from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
from mcp.types import CallToolResult

from client_manager import ClientManager
from helpers import get_ensured_param
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class DeleteFlinkStatementArguments(BaseModel):
    """Arguments for deleting a Flink statement"""
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


class DeleteFlinkStatementHandler(BaseToolHandler):
    """Handler for deleting Flink SQL statements in Confluent Cloud"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Make a request to delete a statement.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Arguments containing statement identification
            session_id: Optional session identifier
            
        Returns:
            Result indicating success or failure of the deletion
        """
        if tool_arguments is None:
            return self.create_response(
                "No arguments provided for Flink statement deletion",
                is_error=True
            )
        
        try:
            # Parse and validate arguments
            args = DeleteFlinkStatementArguments.model_validate(tool_arguments)
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
        
        try:
            # Make the DELETE API call with path parameters
            url = (
                f"/sql/v1/organizations/{organization_id}"
                f"/environments/{environment_id}/statements/{args.statement_name}"
            )
            
            response = await client.delete(url)
            
            if response.status_code >= 400:
                error_text = await response.text()
                return self.create_response(
                    f"Failed to delete Flink SQL statement: {error_text}",
                    is_error=True
                )
            
            return self.create_response(
                f"Flink SQL Statement Deletion Status Code: {response.status_code}"
            )
            
        except Exception as e:
            return self.create_response(
                f"Error deleting Flink SQL statement: {str(e)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.DELETE_FLINK_STATEMENTS,
            description="Make a request to delete a statement.",
            inputSchema=DeleteFlinkStatementArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["FLINK_API_KEY", "FLINK_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True