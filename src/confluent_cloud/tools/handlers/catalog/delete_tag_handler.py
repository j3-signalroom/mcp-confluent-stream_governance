import httpx
from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
from confluent_cloud.tools.base_tools import BaseToolHandler, ToolConfig
from confluent_cloud.tools.tool_name import ToolName
from mcp.types import CallToolResult
from client_manager import ClientManager


class DeleteTagArguments(BaseModel):
    """Arguments for deleting a tag"""
    base_url: HttpUrl | None = Field(
        default=None,
        description="The base URL of the Schema Registry REST API."
    )
    tag_name: str = Field(
        ...,
        description="Name of the tag to delete",
        min_length=1
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("SCHEMA_REGISTRY_ENDPOINT")
        return v
    
class DeleteTagHandler(BaseToolHandler):
    """Handler for deleting tag definitions from Confluent Cloud Schema Registry"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        if tool_arguments is None:
            return self.create_response(
                "No arguments provided for tag deletion",
                is_error=True
            )
        
        try:
            args = DeleteTagArguments.model_validate(tool_arguments)
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
        
        # Make the DELETE API call
        async with httpx.AsyncClient() as client:
            try:
                response = await client.delete(
                    f"{endpoint}/catalog/v1/types/tagdefs/{args.tag_name}",
                    auth=(api_key, api_secret),
                    headers={"Content-Type": "application/json"}
                )
                
                response.raise_for_status()
                
                return self.create_response(
                    f"Successfully deleted tag: {args.tag_name}. Status: {response.status_code}"
                )
                
            except httpx.HTTPStatusError as e:
                return self.create_response(
                    f"Failed to delete tag: {e.response.text}",
                    is_error=True
                )
            except Exception as e:
                return self.create_response(
                    f"Error deleting tag: {str(e)}",
                    is_error=True
                )
    
    def get_tool_config(self) -> ToolConfig:
        return ToolConfig(
            name=ToolName.DELETE_TAG,
            description="Delete a tag definition from Confluent Cloud.",
            inputSchema=DeleteTagArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        return ["SCHEMA_REGISTRY_API_KEY", "SCHEMA_REGISTRY_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        return True