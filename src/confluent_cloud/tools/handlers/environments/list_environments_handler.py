from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
import logging
from mcp.types import CallToolResult

from client_manager import ClientManager
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName

logger = logging.getLogger(__name__)


class ListEnvironmentsArguments(BaseModel):
    """Arguments for listing environments"""
    base_url: HttpUrl | None = Field(
        default=None,
        description="The base URL of the Confluent Cloud REST API."
    )
    page_token: str | None = Field(
        default=None,
        description="Token for the next page of environments"
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("CONFLUENT_CLOUD_REST_ENDPOINT")
        return v


# Response validation schemas
class EnvironmentMetadata(BaseModel):
    """Environment metadata"""
    created_at: str
    updated_at: str
    deleted_at: str | None = None
    resource_name: str
    self: HttpUrl


class StreamGovernanceConfig(BaseModel):
    """Stream governance configuration"""
    package: str


class Environment(BaseModel):
    """Confluent Cloud environment"""
    api_version: str = Field(..., pattern="^org/v2$")
    kind: str = Field(..., pattern="^Environment$")
    id: str
    metadata: EnvironmentMetadata
    display_name: str
    stream_governance_config: StreamGovernanceConfig | None = None


class EnvironmentListMetadata(BaseModel):
    """Pagination metadata for environment list"""
    first: HttpUrl | None = None
    last: HttpUrl | None = None
    prev: HttpUrl | None = None
    next: HttpUrl | None = None
    total_size: int | None = None


class EnvironmentList(BaseModel):
    """List of environments with pagination"""
    api_version: str = Field(..., pattern="^org/v2$")
    kind: str = Field(..., pattern="^EnvironmentList$")
    metadata: EnvironmentListMetadata | None = None
    data: list[Environment]


class EnvironmentSummary(BaseModel):
    """Simplified environment summary for output"""
    id: str
    name: str
    created_at: str
    updated_at: str
    deleted_at: str | None = None
    resource_name: str
    stream_governance: str | None = None


class ListEnvironmentsHandler(BaseToolHandler):
    """Handler for listing environments in Confluent Cloud"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Get all environments in Confluent Cloud with pagination support.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Optional arguments for pagination
            session_id: Optional session identifier
            
        Returns:
            Result containing the list of environments
        """
        try:
            # Parse and validate arguments
            args = ListEnvironmentsArguments.model_validate(tool_arguments or {})
        except Exception as e:
            return self.create_response(
                f"Invalid arguments: {str(e)}",
                is_error=True
            )
        
        try:
            # Update base URL if provided
            if args.base_url is not None and str(args.base_url) != "":
                client_manager.set_confluent_cloud_rest_endpoint(str(args.base_url))
            
            # Get the Confluent Cloud REST client
            client = client_manager.get_confluent_cloud_rest_client()
            
            # Prepare query parameters
            params = {}
            if args.page_token:
                params["page_token"] = args.page_token
            
            # Make the GET API call
            response = await client.get(
                "/org/v2/environments",
                params=params if params else None
            )
            
            if response.status_code >= 400:
                error_text = await response.text()
                logger.error("API Error: %s", error_text)
                return self.create_response(
                    f"Failed to fetch environments: {error_text}",
                    is_error=True,
                    meta={"error": error_text}
                )
            
            # Parse response
            response_data = await response.json()
            
            try:
                # Validate the response structure
                validated_response = EnvironmentList.model_validate(response_data)
                
                # Transform to summary format
                environments = [
                    EnvironmentSummary(
                        id=env.id,
                        name=env.display_name,
                        created_at=env.metadata.created_at,
                        updated_at=env.metadata.updated_at,
                        deleted_at=env.metadata.deleted_at,
                        resource_name=env.metadata.resource_name,
                        stream_governance=env.stream_governance_config.package if env.stream_governance_config else None
                    )
                    for env in validated_response.data
                ]
                
                # Format environment details for display
                environment_details = "\n".join([
                    f"""
Environment: {env.name}
  ID: {env.id}
  Resource Name: {env.resource_name}
  Created At: {env.created_at}
  Updated At: {env.updated_at}"""
                    + (f"\n  Deleted At: {env.deleted_at}" if env.deleted_at else "")
                    + (f"\n  Stream Governance Package: {env.stream_governance}" if env.stream_governance else "")
                    for env in environments
                ])
                
                # Format pagination info
                metadata = validated_response.metadata
                pagination_info = ""
                if metadata:
                    pagination_parts = ["\nPagination:"]
                    if metadata.total_size is not None:
                        pagination_parts.append(f"\n  Total Environments: {metadata.total_size}")
                    if metadata.first:
                        pagination_parts.append(f"\n  First Page: {metadata.first}")
                    if metadata.last:
                        pagination_parts.append(f"\n  Last Page: {metadata.last}")
                    if metadata.prev:
                        pagination_parts.append(f"\n  Previous Page: {metadata.prev}")
                    if metadata.next:
                        pagination_parts.append(f"\n  Next Page: {metadata.next}")
                    pagination_info = "".join(pagination_parts) + "\n"
                
                return self.create_response(
                    f"Successfully retrieved {len(environments)} environments:\n{environment_details}\n{pagination_info}",
                    is_error=False,
                    meta={
                        "environments": [env.model_dump() for env in environments],
                        "total": metadata.total_size if metadata else None,
                        "pagination": {
                            "first": str(metadata.first) if metadata and metadata.first else None,
                            "last": str(metadata.last) if metadata and metadata.last else None,
                            "prev": str(metadata.prev) if metadata and metadata.prev else None,
                            "next": str(metadata.next) if metadata and metadata.next else None,
                        } if metadata else None
                    }
                )
                
            except Exception as validation_error:
                logger.error("Environment list validation error: %s", validation_error)
                return self.create_response(
                    f"Invalid environment list data: {str(validation_error)}",
                    is_error=True,
                    meta={"error": str(validation_error)}
                )
                
        except Exception as error:
            logger.error("Error in ListEnvironmentsHandler: %s", error)
            return self.create_response(
                f"Failed to fetch environments: {str(error)}",
                is_error=True,
                meta={"error": str(error)}
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.LIST_ENVIRONMENTS,
            description="Get all environments in Confluent Cloud with pagination support",
            inputSchema=ListEnvironmentsArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["CONFLUENT_CLOUD_API_KEY", "CONFLUENT_CLOUD_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True