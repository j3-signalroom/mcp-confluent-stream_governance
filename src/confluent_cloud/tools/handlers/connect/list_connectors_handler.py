from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
from mcp.types import CallToolResult

from client_manager import ClientManager
from helpers import get_ensured_param
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class ListConnectorArguments(BaseModel):
    """Arguments for listing connectors"""
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
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("CONFLUENT_CLOUD_REST_ENDPOINT")
        return v
    
    @field_validator('environment_id', 'cluster_id', mode='before')
    @classmethod
    def strip_whitespace(cls, v: str | None) -> str | None:
        """Strip whitespace from string fields"""
        if v is not None and isinstance(v, str):
            return v.strip()
        return v


class ListConnectorsHandler(BaseToolHandler):
    """Handler for listing connectors in Confluent Cloud"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Retrieve a list of names of the active connectors.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Optional arguments for filtering
            session_id: Optional session identifier
            
        Returns:
            Result containing the list of connector names
        """
        try:
            # Parse and validate arguments
            args = ListConnectorArguments.model_validate(tool_arguments or {})
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
                f"/clusters/{kafka_cluster_id}/connectors"
            )
            
            response = await client.get(url)
            
            if response.status_code >= 400:
                error_text = await response.text()
                return self.create_response(
                    f"Failed to list Confluent Cloud connectors for {kafka_cluster_id}: {error_text}",
                    is_error=True
                )
            
            # Response should be a list of connector names
            connector_names = await response.json()
            
            # Validate response is a list
            if not isinstance(connector_names, list):
                return self.create_response(
                    f"Unexpected response format: expected list, got {type(connector_names)}",
                    is_error=True
                )
            
            # Format the response
            connectors_str = ", ".join(connector_names) if connector_names else "None"
            
            return self.create_response(
                f"Active Connectors: {connectors_str}"
            )
            
        except Exception as e:
            return self.create_response(
                f"Error listing connectors: {str(e)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.LIST_CONNECTORS,
            description=(
                'Retrieve a list of "names" of the active connectors. '
                'You can then make a read request for a specific connector by name.'
            ),
            inputSchema=ListConnectorArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["CONFLUENT_CLOUD_API_KEY", "CONFLUENT_CLOUD_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True