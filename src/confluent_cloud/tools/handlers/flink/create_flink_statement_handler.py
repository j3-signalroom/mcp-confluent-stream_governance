from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
import json
from mcp.types import CallToolResult

from client_manager import ClientManager
from helpers import get_ensured_param
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class CreateFlinkStatementArguments(BaseModel):
    """Arguments for creating a Flink statement"""
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
        description="The id associated with the compute pool in context."
    )
    statement: str = Field(
        ...,
        min_length=1,
        max_length=131072,
        description=(
            "The raw Flink SQL text statement. Create table statements may not be necessary "
            "as topics in confluent cloud will be detected as created schemas. Make sure to "
            "show and describe tables before creating new ones."
        )
    )
    statement_name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        pattern=r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$",
        description="The user provided name of the resource, unique within this environment."
    )
    catalog_name: str = Field(
        default="",
        min_length=0,
        description=(
            "The catalog name to be used for the statement. "
            "Typically the confluent environment name."
        )
    )
    database_name: str = Field(
        default="",
        min_length=0,
        description=(
            "The database name to be used for the statement. "
            "Typically the Kafka cluster name."
        )
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("FLINK_REST_ENDPOINT")
        return v
    
    @field_validator('catalog_name', mode='before')
    @classmethod
    def set_default_catalog_name(cls, v: str | None) -> str:
        """Set default catalog_name from environment if not provided"""
        if v is None or v == "":
            return os.getenv("FLINK_ENV_NAME", "")
        return v
    
    @field_validator('database_name', mode='before')
    @classmethod
    def set_default_database_name(cls, v: str | None) -> str:
        """Set default database_name from environment if not provided"""
        if v is None or v == "":
            return os.getenv("FLINK_DATABASE_NAME", "")
        return v
    
    @field_validator(
        'organization_id',
        'environment_id', 
        'compute_pool_id',
        'catalog_name',
        'database_name',
        mode='before'
    )
    @classmethod
    def strip_whitespace(cls, v: str | None) -> str | None:
        """Strip whitespace from string fields"""
        if v is not None and isinstance(v, str):
            return v.strip()
        return v


class CreateFlinkStatementHandler(BaseToolHandler):
    """Handler for creating Flink SQL statements in Confluent Cloud"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Make a request to create a statement.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Arguments for statement creation
            session_id: Optional session identifier
            
        Returns:
            Result containing the created statement details
        """
        if tool_arguments is None:
            return self.create_response(
                "No arguments provided for Flink statement creation",
                is_error=True
            )
        
        try:
            # Parse and validate arguments
            args = CreateFlinkStatementArguments.model_validate(tool_arguments)
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
            compute_pool_id = get_ensured_param(
                "FLINK_COMPUTE_POOL_ID",
                "Compute Pool ID is required",
                args.compute_pool_id
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
        
        # Build properties dictionary - only include catalog and database if defined
        properties = {}
        if args.catalog_name:
            properties["sql.current-catalog"] = args.catalog_name
        if args.database_name:
            properties["sql.current-database"] = args.database_name
        
        # Build the request body
        request_body = {
            "name": args.statement_name,
            "organization_id": organization_id,
            "environment_id": environment_id,
            "spec": {
                "compute_pool_id": compute_pool_id,
                "statement": args.statement,
                "properties": properties
            }
        }
        
        try:
            # Make the POST API call with path parameters
            url = (
                f"/sql/v1/organizations/{organization_id}"
                f"/environments/{environment_id}/statements"
            )
            
            response = await client.post(
                url,
                json=request_body
            )
            
            if response.status_code >= 400:
                error_text = await response.text()
                return self.create_response(
                    f"Failed to create Flink SQL statement: {error_text}",
                    is_error=True
                )
            
            result = await response.json()
            return self.create_response(
                json.dumps(result, indent=2)
            )
            
        except Exception as e:
            return self.create_response(
                f"Error creating Flink SQL statement: {str(e)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.CREATE_FLINK_STATEMENT,
            description="Make a request to create a statement.",
            inputSchema=CreateFlinkStatementArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["FLINK_API_KEY", "FLINK_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True