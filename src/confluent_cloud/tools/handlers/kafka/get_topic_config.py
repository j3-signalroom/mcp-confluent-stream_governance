from typing import Any, Dict, List, Optional
import json
from mcp.types import CallToolResult
from pydantic import BaseModel, Field, field_validator

from client_manager import ClientManager
from helpers import get_ensured_param
from tools.base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName
from env import env


class GetTopicConfigArguments(BaseModel):
    """Arguments for getting Kafka topic configuration."""
    
    base_url: Optional[str] = Field(
        default_factory=lambda: env.KAFKA_REST_ENDPOINT or "",
        description="The base URL of the Confluent Cloud Kafka REST API.",
    )
    cluster_id: Optional[str] = Field(
        default=None,
        description="The unique identifier for the Kafka cluster.",
    )
    topic_name: str = Field(
        ...,
        min_length=1,
        description="Name of the topic to get configuration for",
    )

    @field_validator("topic_name")
    @classmethod
    def validate_topic_name(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("topic_name cannot be empty")
        return v


class GetTopicConfigHandler(BaseToolHandler):
    """
    Handler for retrieving Kafka topic configurations through Confluent Cloud REST API.
    This implementation uses Confluent's REST API endpoints to fetch topic configuration
    details that aren't directly accessible through the native Kafka admin client API.
    """
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: Dict[str, Any],
        session_id: Optional[str] = None,
    ) -> CallToolResult:
        """
        Retrieves configuration details for a specific Kafka topic.
        
        Args:
            client_manager: The client manager for Confluent Cloud REST API
            tool_arguments: The arguments containing cluster ID and topic name
            session_id: Optional session ID (unused for this operation)
            
        Returns:
            A CallToolResult containing topic details and configuration
        """
        # Parse and validate arguments
        args = GetTopicConfigArguments(**tool_arguments)
        
        # Ensure cluster ID is provided
        kafka_cluster_id = get_ensured_param(
            "KAFKA_CLUSTER_ID",
            "Kafka Cluster ID is required",
            args.cluster_id,
        )
        
        # Set base URL if provided
        if args.base_url:
            client_manager.set_confluent_cloud_kafka_rest_endpoint(args.base_url)
        
        # Get the REST client
        rest_client = client_manager.get_confluent_cloud_kafka_rest_client()
        
        try:
            # First, get topic details
            topic_url = f"/kafka/v3/clusters/{kafka_cluster_id}/topics/{args.topic_name}"
            topic_response = await rest_client.get(topic_url)
            
            if not topic_response.ok:
                error_data = await topic_response.json() if topic_response.content else {}
                return self.create_response(
                    f"Failed to retrieve topic details: {json.dumps(error_data)}",
                    is_error=True,
                )
            
            topic_data = await topic_response.json()
            
            # Then, get topic configurations
            config_url = f"/kafka/v3/clusters/{kafka_cluster_id}/topics/{args.topic_name}/configs"
            config_response = await rest_client.get(config_url)
            
            if not config_response.ok:
                error_data = await config_response.json() if config_response.content else {}
                return self.create_response(
                    f"Failed to retrieve topic configuration: {json.dumps(error_data)}",
                    is_error=True,
                )
            
            config_data = await config_response.json()
            
            # Combine topic details and configuration into a single response
            response = {
                "topicDetails": topic_data,
                "topicConfig": config_data,
            }
            
            return self.create_response(
                f"Topic configuration for '{args.topic_name}':\n{json.dumps(response, indent=2)}",
                is_error=False,
            )
            
        except Exception as error:
            return self.create_response(
                f"Failed to retrieve topic configuration: {str(error)}",
                is_error=True,
            )

    def get_tool_config(self) -> ToolConfig:
        """Returns the tool configuration."""
        return ToolConfig(
            name=ToolName.GET_TOPIC_CONFIG,
            description="Retrieve configuration details for a specific Kafka topic.",
            input_schema=GetTopicConfigArguments.model_json_schema(),
        )

    def get_required_env_vars(self) -> List[str]:
        """Returns the required environment variables."""
        return ["KAFKA_API_KEY", "KAFKA_API_SECRET", "BOOTSTRAP_SERVERS"]

    def is_confluent_cloud_only(self) -> bool:
        """Indicates this tool only works with Confluent Cloud."""
        return True