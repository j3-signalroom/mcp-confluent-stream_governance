from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
import json
from mcp.types import CallToolResult

from client_manager import ClientManager
from helpers import get_ensured_param
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class ReadConnectorArguments(BaseModel):
    """Arguments for reading a connector"""
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
        description="The unique name of the connector."
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("CONFLUENT_CLOUD_REST_ENDPOINT")
        return v
    
    @field_validator('environment_id', 'cluster_id', 'connector_name', mode='before')
    @classmethod
    def strip_whitespace(cls, v: str | None) -> str | None:
        """Strip whitespace from string fields"""
        if v is not None and isinstance(v, str):
            return v.strip()
        return v


class ReadConnectorHandler(BaseToolHandler):
    """Handler for reading connector details in Confluent Cloud"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Get information about the connector.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Arguments containing connector identification
            session_id: Optional session identifier
            
        Returns:
            Result containing the connector details
        """
        if tool_arguments is None:
            return self.create_response(
                "No arguments provided for reading connector",
                is_error=True
            )
        
        try:
            # Parse and validate arguments
            args = ReadConnectorArguments.model_validate(tool_arguments)
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
            # Make the GET API call with path parameters
            url = (
                f"/connect/v1/environments/{environment_id}"
                f"/clusters/{kafka_cluster_id}/connectors/{args.connector_name}"
            )
            
            response = await client.get(url)
            
            if response.status_code >= 400:
                error_text = await response.text()
                return self.create_response(
                    f"Failed to get information about connector {args.connector_name}: {error_text}",
                    is_error=True
                )
            
            # Parse the connector details
            connector_details = await response.json()
            
            return self.create_response(
                f"Connector Details for {args.connector_name}: {json.dumps(connector_details, indent=2)}"
            )
            
        except Exception as e:
            return self.create_response(
                f"Error reading connector: {str(e)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.READ_CONNECTOR,
            description="Get information about the connector.",
            inputSchema=ReadConnectorArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["CONFLUENT_CLOUD_API_KEY", "CONFLUENT_CLOUD_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True