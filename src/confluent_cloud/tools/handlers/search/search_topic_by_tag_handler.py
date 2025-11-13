from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
import json
from mcp.types import CallToolResult

from client_manager import ClientManager
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class SearchTopicsByTagArguments(BaseModel):
    """Arguments for searching topics by tag"""
    base_url: HttpUrl | None = Field(
        default=None,
        description="The base URL of the Schema Registry REST API."
    )
    topic_tag: str | None = Field(
        default=None,
        description="The tag we wish to search for"
    )
    limit: int = Field(
        default=100,
        le=500,
        description="The maximum number of topics to return."
    )
    offset: int = Field(
        default=0,
        description="The offset to start the search from. Used for pagination."
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("SCHEMA_REGISTRY_ENDPOINT")
        return v


class SearchTopicsByTagHandler(BaseToolHandler):
    """Handler for searching topics by tag in Schema Registry"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        List all topics in the Kafka cluster with the specified tag.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Arguments for search filtering
            session_id: Optional session identifier
            
        Returns:
            Result containing the search results
        """
        try:
            # Parse and validate arguments
            args = SearchTopicsByTagArguments.model_validate(tool_arguments or {})
        except Exception as e:
            return self.create_response(
                f"Invalid arguments: {str(e)}",
                is_error=True
            )
        
        # Update base URL if provided
        if args.base_url is not None and str(args.base_url) != "":
            client_manager.set_confluent_cloud_schema_registry_endpoint(str(args.base_url))
        
        # Get the Schema Registry REST client
        client = client_manager.get_confluent_cloud_schema_registry_rest_client()
        
        try:
            # Build URL with query parameters embedded in path
            # Note: This API design uses path template with query params
            url = (
                f"/catalog/v1/search/basic"
                f"?types=kafka_topic"
                f"&tag={args.topic_tag if args.topic_tag else ''}"
                f"&limit={args.limit}"
                f"&offset={args.offset}"
            )
            
            response = await client.get(url)
            
            if response.status_code >= 400:
                error_text = await response.text()
                return self.create_response(
                    f"Failed to search for topics by tag: {error_text}",
                    is_error=True
                )
            
            result = await response.json()
            return self.create_response(
                json.dumps(result, indent=2)
            )
            
        except Exception as e:
            return self.create_response(
                f"Error searching for topics by tag: {str(e)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.SEARCH_TOPICS_BY_TAG,
            description="List all topics in the Kafka cluster with the specified tag.",
            inputSchema=SearchTopicsByTagArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["SCHEMA_REGISTRY_API_KEY", "SCHEMA_REGISTRY_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True