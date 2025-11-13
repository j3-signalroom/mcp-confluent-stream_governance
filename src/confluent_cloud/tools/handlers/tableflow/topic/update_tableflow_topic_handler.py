from typing import Any, Literal
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
import json
from mcp.types import CallToolResult

from client_manager import ClientManager
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class TableflowTopicStorageUpdate(BaseModel):
    """Storage configuration for Tableflow topic update"""
    kind: Literal["ByobAws", "Managed"] = Field(
        default="ByobAws",
        description="The storage type either 'Managed' or 'ByobAws'."
    )
    bucket_name: str = Field(
        ...,
        description="The bucket name."
    )
    provider_integration_id: str = Field(
        ...,
        description="The provider integration id."
    )


class TableflowTopicConfigUpdate(BaseModel):
    """Configuration options for Tableflow topic update"""
    retention_ms: str = Field(
        default="6048000000",  # equivalent to 7 days
        description=(
            "The maximum age, in milliseconds, of snapshots (for Iceberg) or versions(for Delta) "
            "to retain in the table for the Tableflow-enabled topic."
        )
    )
    record_failure_strategy: str = Field(
        default="SUSPENDED",
        description=(
            "The strategy to handle record failures in the Tableflow enabled topic "
            "during materialization."
        )
    )


class EnvironmentRefUpdate(BaseModel):
    """Reference to an environment"""
    id: str = Field(
        ...,
        description="The unique identifier for the environment this resource belongs to."
    )


class KafkaClusterRefUpdate(BaseModel):
    """Reference to a Kafka cluster"""
    id: str = Field(
        ...,
        description="ID of the referred resource"
    )
    environment: str = Field(
        ...,
        description="Environment of the referred resource, if env-scoped"
    )


class TableflowTopicUpdateSpec(BaseModel):
    """Specification for Tableflow topic update"""
    storage: TableflowTopicStorageUpdate
    environment: EnvironmentRefUpdate
    kafka_cluster: KafkaClusterRefUpdate
    suspended: bool = Field(
        default=False,
        description=(
            "Indicates whether Tableflow should be suspended. The API allows setting it only "
            "to false i.e resume the Tableflow."
        )
    )
    config: TableflowTopicConfigUpdate = Field(
        default_factory=TableflowTopicConfigUpdate
    )
    table_formats: list[str] = Field(
        default=["ICEBERG"],
        description="The supported table formats for the Tableflow-enabled topic e.g ICEBERG, DELTA"
    )


class UpdateTableflowTopicArguments(BaseModel):
    """Arguments for updating a Tableflow topic"""
    base_url: HttpUrl | None = Field(
        default=None,
        description="The base url of the Tableflow REST API."
    )
    display_name: str = Field(
        ...,
        description="The name of the Kafka topic for which Tableflow is enabled."
    )
    tableflow_topic_config: TableflowTopicUpdateSpec = Field(
        ...,
        description="Configuration for the Tableflow topic update"
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("CONFLUENT_CLOUD_REST_ENDPOINT")
        return v


class UpdateTableFlowTopicHandler(BaseToolHandler):
    """Handler for updating Tableflow topics in Confluent Cloud"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Make a request to update a tableflow topic.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Arguments for Tableflow topic update
            session_id: Optional session identifier
            
        Returns:
            Result containing the updated Tableflow topic details
        """
        if tool_arguments is None:
            return self.create_response(
                "No arguments provided for updating Tableflow topic",
                is_error=True
            )
        
        try:
            # Parse and validate arguments
            args = UpdateTableflowTopicArguments.model_validate(tool_arguments)
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
        config_dict = args.tableflow_topic_config.model_dump()
        
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
            # Make the PATCH API call
            url = f"/tableflow/v1/tableflow-topics/{args.display_name}"
            
            response = await client.patch(
                url,
                json=request_body
            )
            
            if response.status_code >= 400:
                error_text = await response.text()
                return self.create_response(
                    f"Failed to update Tableflow topic for {args.display_name}: {error_text}",
                    is_error=True
                )
            
            result = await response.json()
            return self.create_response(
                f"Tableflow Topic {args.display_name} updated: {json.dumps(result, indent=2)}"
            )
            
        except Exception as e:
            return self.create_response(
                f"Error updating Tableflow topic: {str(e)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.UPDATE_TABLEFLOW_TOPIC,
            description="Make a request to update a tableflow topic.",
            inputSchema=UpdateTableflowTopicArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["TABLEFLOW_API_KEY", "TABLEFLOW_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True