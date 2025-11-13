from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
from mcp.types import CallToolResult

from client_manager import ClientManager
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class SearchTopicsByNameArguments(BaseModel):
    """Arguments for searching topics by name"""
    base_url: HttpUrl | None = Field(
        default=None,
        description="The base URL of the Schema Registry REST API."
    )
    topic_name: str = Field(
        ...,
        description="The topic name to search for"
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("SCHEMA_REGISTRY_ENDPOINT")
        return v


class SearchTopicsByNameHandler(BaseToolHandler):
    """Handler for searching topics by name in Schema Registry"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        List all topics in the Kafka cluster matching the specified name.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Arguments containing topic name to search
            session_id: Optional session identifier
            
        Returns:
            Result containing matching topic names
        """
        if tool_arguments is None:
            return self.create_response(
                "No arguments provided for searching topics by name",
                is_error=True
            )
        
        try:
            # Parse and validate arguments
            args = SearchTopicsByNameArguments.model_validate(tool_arguments)
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
            # Build URL with query parameters
            url = f"/catalog/v1/search/basic?types=kafka_topic&query={args.topic_name}"
            
            response = await client.get(url)
            
            if response.status_code >= 400:
                error_text = await response.text()
                return self.create_response(
                    f"Failed to search for topics by name: {error_text}",
                    is_error=True
                )
            
            result = await response.json()
            
            # Extract qualified names from entities
            entities = result.get("entities", [])
            qualified_names = [
                entity.get("attributes", {}).get("qualifiedName")
                for entity in entities
                if entity.get("attributes", {}).get("qualifiedName")
            ]
            
            # Format response
            if qualified_names:
                response_text = ", ".join(qualified_names)
            else:
                response_text = "No matching topics found"
            
            return self.create_response(response_text)
            
        except Exception as e:
            return self.create_response(
                f"Error searching for topics by name: {str(e)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.SEARCH_TOPICS_BY_NAME,
            description="List all topics in the Kafka cluster matching the specified name.",
            inputSchema=SearchTopicsByNameArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["SCHEMA_REGISTRY_API_KEY", "SCHEMA_REGISTRY_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True