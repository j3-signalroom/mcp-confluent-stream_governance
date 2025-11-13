import httpx
from typing import Any
import json
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os

from client_manager import ClientManager
from mcp.types import CallToolResult
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class ListTagsArguments(BaseModel):
    """Arguments for listing tags"""
    base_url: HttpUrl | None = Field(
        default=None,
        description="The base URL of the Schema Registry REST API."
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("SCHEMA_REGISTRY_ENDPOINT")
        return v
class ListTagsHandler(BaseToolHandler):
    """Handler for listing all tag definitions from Confluent Cloud Schema Registry"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        # Parse arguments (baseUrl is optional)
        try:
            args = ListTagsArguments.model_validate(tool_arguments or {})
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
        
        # Make the GET API call
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{endpoint}/catalog/v1/types/tagdefs",
                    auth=(api_key, api_secret),
                    headers={"Content-Type": "application/json"}
                )
                
                response.raise_for_status()
                result = response.json()
                
                return self.create_response(
                    f"Successfully retrieved tags: {json.dumps(result, indent=2)}"
                )
                
            except httpx.HTTPStatusError as e:
                return self.create_response(
                    f"Failed to list tags: {e.response.text}",
                    is_error=True
                )
            except Exception as e:
                return self.create_response(
                    f"Error listing tags: {str(e)}",
                    is_error=True
                )
    
    def get_tool_config(self) -> ToolConfig:
        return ToolConfig(
            name=ToolName.LIST_TAGS,
            description="Retrieve all tags with definitions from Confluent Cloud Schema Registry.",
            inputSchema=ListTagsArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        return ["SCHEMA_REGISTRY_API_KEY", "SCHEMA_REGISTRY_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        return True