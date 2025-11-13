from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
from mcp.types import CallToolResult

from client_manager import ClientManager
from helpers import get_ensured_param
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class DeleteConnectorArguments(BaseModel):
    """Arguments for deleting a connector"""
    base_url: HttpUrl | None = Field(
        default=None,
        description="The base URL of the Kafka Connect REST API."
    )
    environment_id: str | None = Field(
        default=None,
        description="The unique identifier for the environment this resource belongs to."
    )
    cluster_id: str | None = Field(
        default=None,
        description="The unique identifier for the Kafka cluster."
    )
    connector_name: str = Field(
        ...,
        min_length=1,
        description="The name of the connector to delete."
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("CONFLUENT_CLOUD_REST_ENDPOINT")
        return v


class DeleteConnectorHandler(BaseToolHandler):
    """Handler for deleting connectors in Confluent Cloud"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Delete an existing connector.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Arguments for connector deletion
            session_id: Optional session identifier
            
        Returns:
            Result indicating success or failure of the deletion
        """
        if tool_arguments is None:
            return self.create_response(
                "No arguments provided for connector deletion",
                is_error=True
            )
        
        try:
            # Parse and validate arguments
            args = DeleteConnectorArguments.model_validate(tool_arguments)
        except Exception as e:
            return self.create_response(
                f"Invalid arguments: {str(e)}",
                is_error=True
            )
        
        # Get ensured parameters with fallback to env vars
        try:
            environment_id = get_ensured_param(
                "KAFKA_ENV_ID",
                "Environment ID is required",
                args.environment_id
            )
            kafka_cluster_id = get_ensured_param(
                "KAFKA_CLUSTER_ID",
                "Kafka Cluster ID is required",
                args.cluster_id
            )
        except ValueError as e:
            return self.create_response(
                str(e),
                is_error=True
            )
        
        # Update base URL if provided
        if args.base_url is not None and str(args.base_url) != "":
            client_manager.set_confluent_cloud_rest_endpoint(str(args.base_url))
        
        # Get the Confluent Cloud REST client
        client = client_manager.get_confluent_cloud_rest_client()
        
        try:
            # Make the DELETE API call with path parameters
            url = (
                f"/connect/v1/environments/{environment_id}"
                f"/clusters/{kafka_cluster_id}/connectors/{args.connector_name}"
            )
            
            response = await client.delete(url)
            
            if response.status_code >= 400:
                error_text = await response.text()
                return self.create_response(
                    f"Failed to delete connector {args.connector_name}: {error_text}",
                    is_error=True
                )
            
            return self.create_response(
                f"Successfully deleted connector {args.connector_name}"
            )
            
        except Exception as e:
            return self.create_response(
                f"Error deleting connector: {str(e)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.DELETE_CONNECTOR,
            description="Delete an existing connector. Returns success message if deletion was successful.",
            inputSchema=DeleteConnectorArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["CONFLUENT_CLOUD_API_KEY", "CONFLUENT_CLOUD_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True