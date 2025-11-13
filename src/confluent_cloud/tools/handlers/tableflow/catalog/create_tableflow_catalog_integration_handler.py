from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
import json
from mcp.types import CallToolResult

from client_manager import ClientManager
from helpers import get_ensured_param
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class CatalogIntegrationConfig(BaseModel):
    """Configuration for catalog integration"""
    kind: str = Field(
        default="AwsGlue",
        description="The type of the catalog integration."
    )
    provider_integration_id: str = Field(
        ...,
        description="The provider integration id."
    )


class TableflowCatalogIntegrationConfig(BaseModel):
    """Configuration for Tableflow Catalog Integration"""
    display_name: str = Field(
        ...,
        description="The name of the Kafka topic for which Tableflow is enabled."
    )
    config: CatalogIntegrationConfig
    suspended: bool = Field(
        default=False,
        description=(
            "Indicates whether Tableflow Catalog Integration should be suspended. "
            "The API allows setting it only to false i.e resume the Catalog Integration."
        )
    )


class CreateTableflowCatalogIntegrationArguments(BaseModel):
    """Arguments for creating a Tableflow Catalog Integration"""
    base_url: HttpUrl | None = Field(
        default=None,
        description="The base url of the Tableflow REST API."
    )
    tableflow_catalog_integration_config: TableflowCatalogIntegrationConfig = Field(
        ...,
        description="Configuration for the catalog integration"
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("CONFLUENT_CLOUD_REST_ENDPOINT")
        return v if isinstance(v, str) else None


class CreateTableFlowCatalogIntegrationHandler(BaseToolHandler):
    """Handler for creating Tableflow Catalog Integrations in Confluent Cloud"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Make a request to create a catalog integration.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Arguments for catalog integration creation
            session_id: Optional session identifier
            
        Returns:
            Result containing the created catalog integration details
        """
        if tool_arguments is None:
            return self.create_response(
                "No arguments provided for creating Tableflow Catalog Integration",
                is_error=True
            )
        
        try:
            # Parse and validate arguments
            args = CreateTableflowCatalogIntegrationArguments.model_validate(tool_arguments)
        except Exception as e:
            return self.create_response(
                f"Invalid arguments: {str(e)}",
                is_error=True
            )
        
        # Get ensured parameters with fallback to env vars
        try:
            environment_id = get_ensured_param(
                "KAFKA_ENV_ID",
                "Environment ID is required"
            )
            kafka_cluster_id = get_ensured_param(
                "KAFKA_CLUSTER_ID",
                "Kafka Cluster ID is required"
            )
        except ValueError as e:
            return self.create_response(
                str(e),
                is_error=True
            )
        
        # Update base URL if provided
        if args.base_url is not None and str(args.base_url) != "":
            client_manager.set_confluent_cloud_tableflow_rest_endpoint(str(args.base_url))
        
        # Get the Tableflow REST client
        client = client_manager.get_confluent_cloud_tableflow_rest_client()
        
        # Build the request body
        # Convert the config to dict and merge with environment/cluster info
        config_dict = args.tableflow_catalog_integration_config.model_dump()
        
        request_body = {
            "spec": {
                "environment": {
                    "id": environment_id,
                    # Only include id, as the general environment object also requires 
                    # readonly fields like resource_name
                },
                "kafka_cluster": {
                    "id": kafka_cluster_id,
                    "environment": environment_id,
                },
                **config_dict  # Spread the config (display_name, config, suspended)
            }
        }
        
        try:
            # Make the POST API call
            response = await client.post(
                "/tableflow/v1/catalog-integrations",
                json=request_body
            )
            
            if response.status_code >= 400:
                error_text = await response.text()
                return self.create_response(
                    f"Failed to create Tableflow Catalog Integration for "
                    f"{args.tableflow_catalog_integration_config.display_name}: {error_text}",
                    is_error=True
                )
            
            result = await response.json()
            return self.create_response(
                f"Tableflow Catalog Integration {args.tableflow_catalog_integration_config.display_name} "
                f"created: {json.dumps(result, indent=2)}"
            )
            
        except Exception as e:
            return self.create_response(
                f"Error creating Tableflow Catalog Integration: {str(e)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.CREATE_TABLEFLOW_CATALOG_INTEGRATION,
            description="Make a request to create a catalog integration.",
            inputSchema=CreateTableflowCatalogIntegrationArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["TABLEFLOW_API_KEY", "TABLEFLOW_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True