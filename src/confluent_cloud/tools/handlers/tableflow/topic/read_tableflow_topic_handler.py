from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
import json
from mcp.types import CallToolResult

from client_manager import ClientManager
from helpers import get_ensured_param
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class ReadTableflowTopicArguments(BaseModel):
    """Arguments for reading a Tableflow topic"""
    base_url: HttpUrl | None = Field(
        default=None,
        description="The base url of the Tableflow REST API."
    )
    display_name: str = Field(
        ...,
        description="The name of the Kafka topic for which Tableflow is enabled."
    )
    environment_id: str | None = Field(
        default=None,
        description="Scope the operation to the given environment."
    )
    cluster_id: str | None = Field(
        default=None,
        description="Scope the operation to the give Kafka cluster."
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


class ReadTableFlowTopicHandler(BaseToolHandler):
    """Handler for reading Tableflow topic details in Confluent Cloud"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Make a request to read a tableflow topic.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Arguments containing topic name
            session_id: Optional session identifier
            
        Returns:
            Result containing the Tableflow topic details
        """
        if tool_arguments is None:
            return self.create_response(
                "No arguments provided for reading Tableflow topic",
                is_error=True
            )
        
        try:
            # Parse and validate arguments
            args = ReadTableflowTopicArguments.model_validate(tool_arguments)
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
            client_manager.set_confluent_cloud_tableflow_rest_endpoint(str(args.base_url))
        
        # Get the Tableflow REST client
        client = client_manager.get_confluent_cloud_tableflow_rest_client()
        
        try:
            # Build URL with query parameters
            url = (
                f"/tableflow/v1/tableflow-topics/{args.display_name}"
                f"?environment={environment_id}"
                f"&spec.kafka_cluster={kafka_cluster_id}"
            )
            
            response = await client.get(url)
            
            if response.status_code >= 400:
                error_text = await response.text()
                return self.create_response(
                    f"Failed to read Tableflow topic {args.display_name}: {error_text}",
                    is_error=True
                )
            
            result = await response.json()
            return self.create_response(
                f"Tableflow Topic: {json.dumps(result, indent=2)}"
            )
            
        except Exception as e:
            return self.create_response(
                f"Error reading Tableflow topic: {str(e)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.READ_TABLEFLOW_TOPIC,
            description="Make a request to read a tableflow topic.",
            inputSchema=ReadTableflowTopicArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["TABLEFLOW_API_KEY", "TABLEFLOW_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True