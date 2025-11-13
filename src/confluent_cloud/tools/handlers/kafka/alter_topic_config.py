from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
from enum import Enum
import os
import json
from mcp.types import CallToolResult

from client_manager import ClientManager
from helpers import get_ensured_param
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class ConfigOperation(str, Enum):
    """Operation type for topic configuration"""
    SET = "SET"
    DELETE = "DELETE"


class TopicConfigUpdate(BaseModel):
    """Configuration update for a topic"""
    name: str = Field(
        ...,
        min_length=1,
        description="Configuration parameter name"
    )
    value: str = Field(
        ...,
        description="Configuration parameter value"
    )
    operation: ConfigOperation = Field(
        ...,
        description="Operation type"
    )


class AlterTopicConfigArguments(BaseModel):
    """Arguments for altering topic configuration"""
    base_url: HttpUrl | None = Field(
        default=None,
        description="The base URL of the Confluent Cloud Kafka REST API."
    )
    cluster_id: str | None = Field(
        default=None,
        description="The unique identifier for the Kafka cluster."
    )
    topic_name: str = Field(
        ...,
        min_length=1,
        description="Name of the topic to alter"
    )
    topic_configs: list[TopicConfigUpdate] = Field(
        ...,
        min_length=1,
        description="Array of configuration updates to apply"
    )
    validate_only: bool = Field(
        default=False,
        description="If true, validate the request without applying changes"
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("KAFKA_REST_ENDPOINT")
        return v


class AlterTopicConfigHandler(BaseToolHandler):
    """
    Handler for altering Kafka topic configurations through Confluent Cloud REST API.
    
    This implementation serves as a workaround since the native Kafka
    admin client API does not provide direct methods for altering topic configurations.
    Instead, we utilize Confluent's REST API endpoints to achieve this functionality.
    """
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Alter topic configuration in Confluent Cloud.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Arguments containing topic and config updates
            session_id: Optional session identifier
            
        Returns:
            Result of the configuration update
        """
        if tool_arguments is None:
            return self.create_response(
                "No arguments provided for altering topic config",
                is_error=True
            )
        
        try:
            # Parse and validate arguments
            args = AlterTopicConfigArguments.model_validate(tool_arguments)
        except Exception as e:
            return self.create_response(
                f"Invalid arguments: {str(e)}",
                is_error=True
            )
        
        # Get ensured parameters with fallback to env vars
        try:
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
            client_manager.set_confluent_cloud_kafka_rest_endpoint(str(args.base_url))
        
        # Get the Kafka REST client
        client = client_manager.get_confluent_cloud_kafka_rest_client()
        
        # Build the request body
        request_body = {
            "data": [config.model_dump() for config in args.topic_configs],
            "validate_only": args.validate_only
        }
        
        try:
            # Make the POST API call
            # Note: The URL ends with :alter which is a custom action endpoint
            url = (
                f"/kafka/v3/clusters/{kafka_cluster_id}"
                f"/topics/{args.topic_name}/configs:alter"
            )
            
            response = await client.post(
                url,
                json=request_body
            )
            
            if response.status_code >= 400:
                error_text = await response.text()
                return self.create_response(
                    f"Failed to alter topic config: {error_text}",
                    is_error=True
                )
            
            result = await response.json()
            return self.create_response(
                f"Successfully altered topic config: {json.dumps(result, indent=2)}"
            )
            
        except Exception as e:
            return self.create_response(
                f"Error altering topic config: {str(e)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.ALTER_TOPIC_CONFIG,
            description="Alter topic configuration in Confluent Cloud.",
            inputSchema=AlterTopicConfigArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["KAFKA_API_KEY", "KAFKA_API_SECRET", "BOOTSTRAP_SERVERS"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True