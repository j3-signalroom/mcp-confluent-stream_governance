import httpx
from typing import Any
import os
import json

from client_manager import ClientManager
from mcp.types import CallToolResult
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName
from pydantic import BaseModel, Field, HttpUrl, field_validator


class TagDefinition(BaseModel):
    """Definition for a single tag"""
    tag_name: str = Field(..., description="Name of the tag to create", min_length=1)
    description: str = Field(
        default="Tag created via API",
        description="Description for the tag"
    )


class CreateTagsArguments(BaseModel):
    """Arguments for creating topic tags - this is the Zod equivalent"""
    base_url: HttpUrl | None = Field(
        default=None,
        description="The base URL of the Schema Registry REST API."
    )
    tags: list[TagDefinition] = Field(
        ...,
        description="Array of tag definitions to create",
        min_length=1
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("SCHEMA_REGISTRY_ENDPOINT")
        return v

class CreateTopicTagsHandler(BaseToolHandler):
    """Handler for creating topic tags in Confluent Cloud Schema Registry"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        if tool_arguments is None:
            return self.create_response(
                "No arguments provided for tag creation",
                is_error=True
            )
        
        try:
            args = CreateTagsArguments.model_validate(tool_arguments)
        except Exception as e:
            return self.create_response(
                f"Invalid arguments: {str(e)}",
                is_error=True
            )
        
        # Update base URL if provided
        base_url = str(args.base_url) if args.base_url else None
        if base_url:
            client_manager.set_confluent_cloud_schema_registry_endpoint(base_url)
        
        # Prepare tag definitions
        tag_definitions = [
            {
                "entityTypes": ["kafka_topic"],
                "name": tag.tag_name,
                "description": tag.description,
            }
            for tag in args.tags
        ]
        
        # Get credentials and endpoint
        endpoint = client_manager.get_schema_registry_endpoint()
        api_key = os.getenv("SCHEMA_REGISTRY_API_KEY")
        api_secret = os.getenv("SCHEMA_REGISTRY_API_SECRET")
        
        # Make the API call with httpx
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{endpoint}/catalog/v1/types/tagdefs",
                    json=tag_definitions,
                    auth=(api_key, api_secret),
                    headers={"Content-Type": "application/json"}
                )
                
                response.raise_for_status()
                result = response.json()
                
                return self.create_response(
                    f"Successfully created tags: {json.dumps(result, indent=2)}"
                )
                
            except httpx.HTTPStatusError as e:
                return self.create_response(
                    f"Failed to create tags: {e.response.text}",
                    is_error=True
                )
            except Exception as e:
                return self.create_response(
                    f"Error creating tags: {str(e)}",
                    is_error=True
                )
    
    def get_tool_config(self) -> ToolConfig:
        return ToolConfig(
            name=ToolName.CREATE_TOPIC_TAGS,
            description="Create new tag definitions in Confluent Cloud.",
            inputSchema=CreateTagsArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        return ["SCHEMA_REGISTRY_API_KEY", "SCHEMA_REGISTRY_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        return True