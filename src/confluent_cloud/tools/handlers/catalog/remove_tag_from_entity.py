import httpx
from urllib.parse import quote
from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os

from client_manager import ClientManager
from mcp.types import CallToolResult
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class RemoveTagFromEntityArguments(BaseModel):
    """Arguments for removing a tag from an entity"""
    base_url: HttpUrl | None = Field(
        default=None,
        description="The base URL of the Schema Registry REST API."
    )
    tag_name: str = Field(
        ...,
        description="Name of the tag to remove from the entity.",
        min_length=1
    )
    type_name: str = Field(
        default="kafka_topic",
        description="Type of the entity",
        min_length=1
    )
    qualified_name: str = Field(
        ...,
        description=(
            f"Qualified name of the entity. If not provided, you can obtain it from using the "
            f"{ToolName.SEARCH_TOPICS_BY_TAG.value} tool. example: \"lsrc-g2p81:lkc-xq8k7g:my-flights\""
        ),
        min_length=1
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("SCHEMA_REGISTRY_ENDPOINT")
        return v


class RemoveTagFromEntityHandler(BaseToolHandler):
    """Handler for removing tags from entities in Confluent Cloud Schema Registry"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        if tool_arguments is None:
            return self.create_response(
                "No arguments provided for removing tag from entity",
                is_error=True
            )
        
        try:
            args = RemoveTagFromEntityArguments.model_validate(tool_arguments)
        except Exception as e:
            return self.create_response(
                f"Invalid arguments: {str(e)}",
                is_error=True
            )
        
        # Update base URL if provided
        base_url = str(args.base_url) if args.base_url else None
        if base_url:
            client_manager.set_confluent_cloud_schema_registry_endpoint(base_url)
        
        # Get credentials and endpoint
        endpoint = client_manager.get_schema_registry_endpoint()
        api_key = os.getenv("SCHEMA_REGISTRY_API_KEY")
        api_secret = os.getenv("SCHEMA_REGISTRY_API_SECRET")
        
        # URL encode path parameters to handle special characters
        type_name_encoded = quote(args.type_name, safe='')
        qualified_name_encoded = quote(args.qualified_name, safe='')
        tag_name_encoded = quote(args.tag_name, safe='')
        
        # Build URL with encoded parameters
        url = (
            f"{endpoint}/catalog/v1/entity/type/{type_name_encoded}"
            f"/name/{qualified_name_encoded}/tags/{tag_name_encoded}"
        )
        
        # Make the DELETE API call
        async with httpx.AsyncClient() as client:
            try:
                response = await client.delete(
                    url,
                    auth=(api_key, api_secret),
                    headers={"Content-Type": "application/json"}
                )
                
                response.raise_for_status()
                
                return self.create_response(
                    f"Successfully removed tag {args.tag_name} from entity "
                    f"{args.qualified_name} with type {args.type_name}. "
                    f"Status: {response.status_code}"
                )
                
            except httpx.HTTPStatusError as e:
                return self.create_response(
                    f"Failed to remove tag from entity: {e.response.text}",
                    is_error=True
                )
            except Exception as e:
                return self.create_response(
                    f"Error removing tag from entity: {str(e)}",
                    is_error=True
                )
    
    def get_tool_config(self) -> ToolConfig:
        return ToolConfig(
            name=ToolName.REMOVE_TAG_FROM_ENTITY,
            description="Remove tag from an entity in Confluent Cloud.",
            inputSchema=RemoveTagFromEntityArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        return ["SCHEMA_REGISTRY_API_KEY", "SCHEMA_REGISTRY_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        return True