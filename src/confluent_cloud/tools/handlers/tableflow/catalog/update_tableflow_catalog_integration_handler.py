from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
import json
from mcp.types import CallToolResult

from client_manager import ClientManager
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class EnvironmentRef(BaseModel):
    """Reference to an environment"""
    id: str = Field(
        ...,
        description="The unique identifier for the environment this resource belongs to."
    )


class KafkaClusterRef(BaseModel):
    """Reference to a Kafka cluster"""
    id: str = Field(
        ...,
        description="ID of the referred resource"
    )
    environment: str = Field(
        ...,
        description="Environment of the referred resource, if env-scoped"
    )


class CatalogIntegrationUpdateConfig(BaseModel):
    """Configuration for catalog integration update"""
    kind: str = Field(
        default="AwsGlue",
        description="The type of the catalog integration. AwsGlue, Snowflake"
    )


class TableflowCatalogIntegrationUpdateConfig(BaseModel):
    """Configuration for updating a Tableflow Catalog Integration"""
    display_name: str = Field(
        ...,
        description="The name of the Kafka topic for which Tableflow is enabled."
    )
    environment: EnvironmentRef
    kafka_cluster: KafkaClusterRef
    config: CatalogIntegrationUpdateConfig
    suspended: bool = Field(
        default=False,
        description=(
            "Indicates whether Tableflow Catalog Integration should be suspended. "
            "The API allows setting it only to false i.e resume the Catalog Integration."
        )
    )


class UpdateTableflowCatalogIntegrationArguments(BaseModel):
    """Arguments for updating a Tableflow Catalog Integration"""
    base_url: HttpUrl | None = Field(
        default=None,
        description="The base url of the Tableflow REST API."
    )
    tableflow_catalog_integration_config: TableflowCatalogIntegrationUpdateConfig = Field(
        ...,
        description="Configuration for the catalog integration update"
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("CONFLUENT_CLOUD_REST_ENDPOINT")
        return v


class UpdateTableFlowCatalogIntegrationHandler(BaseToolHandler):
    """Handler for updating Tableflow Catalog Integrations in Confluent Cloud"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Make a request to update a catalog integration.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Arguments for catalog integration update
            session_id: Optional session identifier
            
        Returns:
            Result containing the updated catalog integration details
        """
        if tool_arguments is None:
            return self.create_response(
                "No arguments provided for updating Tableflow Catalog Integration",
                is_error=True
            )
        
        try:
            # Parse and validate arguments
            args = UpdateTableflowCatalogIntegrationArguments.model_validate(tool_arguments)
        except Exception as e:
            return self.create_response(
                f"Invalid arguments: {str(e)}",
                is_error=True
            )
        
        # Update base URL if provided
        if args.base_url is not None and str(args.base_url) != "":
            client_manager.set_confluent_cloud_tableflow_rest_endpoint(str(args.base_url))
        
        # Get the Tableflow REST client
        client = client_manager.get_confluent_cloud_tableflow_rest_client()
        
        # Convert config to dict
        config_dict = args.tableflow_catalog_integration_config.model_dump()
        
        # Extract environment separately
        environment = config_dict.pop("environment")
        
        # Build the request body
        # Spread the rest of the config and only include environment.id
        request_body = {
            "spec": {
                **config_dict,
                "environment": {
                    "id": environment["id"]
                    # Only include id, as the general environment object also requires
                    # readonly fields like resource_name
                }
            }
        }
        
        try:
            # Make the POST API call
            # Note: This appears to use POST like create, not PATCH/PUT
            response = await client.post(
                "/tableflow/v1/catalog-integrations",
                json=request_body
            )
            
            if response.status_code >= 400:
                error_text = await response.text()
                return self.create_response(
                    f"Failed to update Tableflow Catalog Integration for "
                    f"{args.tableflow_catalog_integration_config.display_name}: {error_text}",
                    is_error=True
                )
            
            result = await response.json()
            return self.create_response(
                f"Tableflow Catalog Integration {args.tableflow_catalog_integration_config.display_name} "
                f"updated: {json.dumps(result, indent=2)}"
            )
            
        except Exception as e:
            return self.create_response(
                f"Error updating Tableflow Catalog Integration: {str(e)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.UPDATE_TABLEFLOW_CATALOG_INTEGRATION,
            description="Make a request to update a catalog integration.",
            inputSchema=UpdateTableflowCatalogIntegrationArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["TABLEFLOW_API_KEY", "TABLEFLOW_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True