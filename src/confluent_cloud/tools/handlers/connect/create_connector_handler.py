from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
import json
from mcp.types import CallToolResult

from client_manager import ClientManager
from helpers import get_ensured_param
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class ConnectorConfig(BaseModel):
    """Configuration for a connector - allows additional properties"""
    # Required fields
    connector_class: str = Field(
        ...,
        alias="connector.class",
        description=(
            "Required for Managed Connector, Ignored for Custom Connector. "
            "The connector class name, e.g., BigQuerySink, GcsSink, etc."
        )
    )
    
    # Optional fields
    confluent_connector_type: str | None = Field(
        default="MANAGED",
        alias="confluent.connector.type",
        description="Required for Custom Connector. The connector type"
    )
    
    confluent_custom_plugin_id: str | None = Field(
        default=None,
        alias="confluent.custom.plugin.id",
        description="Required for Custom Connector. The custom plugin id of custom connector"
    )
    
    confluent_custom_connection_endpoints: str | None = Field(
        default=None,
        alias="confluent.custom.connection.endpoints",
        description="Optional for Custom Connector. Egress endpoint(s) for the connector"
    )
    
    confluent_custom_schema_registry_auto: str | None = Field(
        default="FALSE",
        alias="confluent.custom.schema.registry.auto",
        description="Optional for Custom Connector. Automatically add required schema registry properties"
    )
    
    class Config:
        # Allow additional string properties (equivalent to .catchall(z.string()))
        extra = "allow"
        populate_by_name = True  # Allow both alias and field name


class CreateConnectorArguments(BaseModel):
    """Arguments for creating a connector"""
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
        description="The name of the connector to create."
    )
    connector_config: ConnectorConfig = Field(
        ...,
        description="Configuration for the connector"
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("CONFLUENT_CLOUD_REST_ENDPOINT")
        return v


class CreateConnectorHandler(BaseToolHandler):
    """Handler for creating connectors in Confluent Cloud"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Create a new connector.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Arguments for connector creation
            session_id: Optional session identifier
            
        Returns:
            Result containing the new connector information
        """
        if tool_arguments is None:
            return self.create_response(
                "No arguments provided for connector creation",
                is_error=True
            )
        
        try:
            # Parse and validate arguments
            args = CreateConnectorArguments.model_validate(tool_arguments)
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
            # Get required credentials
            kafka_api_key = get_ensured_param(
                "KAFKA_API_KEY",
                "Kafka API Key is required to create the connector. Check if env vars are properly set"
            )
            kafka_api_secret = get_ensured_param(
                "KAFKA_API_SECRET",
                "Kafka API Secret is required to create the connector. Check if env vars are properly set"
            )
        except ValueError as e:
            return self.create_response(
                str(e),
                is_error=True
            )
        
        # Build the request body
        # Convert connector_config to dict and merge with required fields
        connector_config_dict = args.connector_config.model_dump(by_alias=True, exclude_none=True)
        
        request_body = {
            "name": args.connector_name,
            "config": {
                "name": args.connector_name,
                "kafka.api.key": kafka_api_key,
                "kafka.api.secret": kafka_api_secret,
                **connector_config_dict  # Spread the connector config
            }
        }
        
        try:
            # Make the POST API call with path parameters
            url = (
                f"/connect/v1/environments/{environment_id}"
                f"/clusters/{kafka_cluster_id}/connectors"
            )
            
            response = await client.post(
                url,
                json=request_body
            )
            
            if response.status_code >= 400:
                error_text = await response.text()
                return self.create_response(
                    f"Failed to create connector {args.connector_name}: {error_text}",
                    is_error=True
                )
            
            result = await response.json()
            return self.create_response(
                f"{args.connector_name} created: {json.dumps(result, indent=2)}"
            )
            
        except Exception as e:
            return self.create_response(
                f"Error creating connector: {str(e)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.CREATE_CONNECTOR,
            description="Create a new connector. Returns the new connector information if successful.",
            inputSchema=CreateConnectorArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return [
            "CONFLUENT_CLOUD_API_KEY",
            "CONFLUENT_CLOUD_API_SECRET",
            "KAFKA_API_KEY",
            "KAFKA_API_SECRET",
        ]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True