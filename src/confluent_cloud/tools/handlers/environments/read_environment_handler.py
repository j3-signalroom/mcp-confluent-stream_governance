from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
import logging
from mcp.types import CallToolResult

from client_manager import ClientManager
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


from list_environments_handler import Environment

logger = logging.getLogger(__name__)


class ReadEnvironmentArguments(BaseModel):
    """Arguments for reading an environment"""
    base_url: HttpUrl | None = Field(
        default=None,
        description="The base URL of the Confluent Cloud REST API."
    )
    environment_id: str = Field(
        ...,
        min_length=1,
        description="The ID of the environment to retrieve"
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("CONFLUENT_CLOUD_REST_ENDPOINT")
        return v


class EnvironmentDetails(BaseModel):
    """Detailed environment information for output"""
    api_version: str
    kind: str
    id: str
    name: str
    metadata: dict[str, Any]
    stream_governance_package: str | None = None


class ReadEnvironmentHandler(BaseToolHandler):
    """Handler for reading a specific environment in Confluent Cloud"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Get details of a specific environment by ID.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Arguments containing environment ID
            session_id: Optional session identifier
            
        Returns:
            Result containing the environment details
        """
        if tool_arguments is None:
            return self.create_response(
                "No arguments provided for reading environment",
                is_error=True
            )
        
        try:
            # Parse and validate arguments
            args = ReadEnvironmentArguments.model_validate(tool_arguments)
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
            
            # Make the GET API call with path parameter
            response = await client.get(f"/org/v2/environments/{args.environment_id}")
            
            if response.status_code >= 400:
                error_text = await response.text()
                logger.error("API Error: %s", error_text)
                return self.create_response(
                    f"Failed to fetch environment: {error_text}",
                    is_error=True,
                    meta={"error": error_text}
                )
            
            # Parse response
            response_data = await response.json()
            
            try:
                # Validate the response structure
                validated_environment = Environment.model_validate(response_data)
                
                # Build environment details
                environment_details = EnvironmentDetails(
                    api_version=validated_environment.api_version,
                    kind=validated_environment.kind,
                    id=validated_environment.id,
                    name=validated_environment.display_name,
                    metadata={
                        "created_at": validated_environment.metadata.created_at,
                        "updated_at": validated_environment.metadata.updated_at,
                        "deleted_at": validated_environment.metadata.deleted_at,
                        "resource_name": validated_environment.metadata.resource_name,
                        "self": str(validated_environment.metadata.self),
                    },
                    stream_governance_package=(
                        validated_environment.stream_governance_config.package
                        if validated_environment.stream_governance_config
                        else None
                    )
                )
                
                # Format details for display
                formatted_details = f"""
Environment: {environment_details.name}
  API Version: {environment_details.api_version}
  Kind: {environment_details.kind}
  ID: {environment_details.id}
  Resource Name: {environment_details.metadata['resource_name']}
  Self Link: {environment_details.metadata['self']}
  Created At: {environment_details.metadata['created_at']}
  Updated At: {environment_details.metadata['updated_at']}"""
                
                # Add optional fields
                if environment_details.metadata.get('deleted_at'):
                    formatted_details += f"\n  Deleted At: {environment_details.metadata['deleted_at']}"
                if environment_details.stream_governance_package:
                    formatted_details += f"\n  Stream Governance Package: {environment_details.stream_governance_package}"
                
                formatted_details += "\n"
                
                return self.create_response(
                    f"Successfully retrieved environment:\n{formatted_details}",
                    is_error=False,
                    meta={"environment": environment_details.model_dump()}
                )
                
            except Exception as validation_error:
                logger.error("Environment validation error: %s", validation_error)
                return self.create_response(
                    f"Invalid environment data: {str(validation_error)}",
                    is_error=True,
                    meta={"error": str(validation_error)}
                )
                
        except Exception as error:
            logger.error("Error in ReadEnvironmentHandler: %s", error)
            return self.create_response(
                f"Failed to fetch environment: {str(error)}",
                is_error=True,
                meta={"error": str(error)}
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.READ_ENVIRONMENT,
            description="Get details of a specific environment by ID",
            inputSchema=ReadEnvironmentArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["CONFLUENT_CLOUD_API_KEY", "CONFLUENT_CLOUD_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True
