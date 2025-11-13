from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
import json
from mcp.types import CallToolResult

from client_manager import ClientManager
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class ListTableFlowRegionsArguments(BaseModel):
    """Arguments for listing Tableflow regions"""
    base_url: HttpUrl | None = Field(
        default=None,
        description="The base url of the Tableflow REST API."
    )
    cloud: str | None = Field(
        default=None,
        description="Filter the results by exact match for cloud."
    )
    page_size: str = Field(
        default="10",
        description="The pagination size of collection requests."
    )
    page_token: str = Field(
        default="0",
        description="An opaque pagination token for collection requests."
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("CONFLUENT_CLOUD_REST_ENDPOINT")
        return v
    
    @field_validator('cloud', 'page_size', 'page_token', mode='before')
    @classmethod
    def strip_whitespace(cls, v: str | None) -> str | None:
        """Strip whitespace from string fields"""
        if v is not None and isinstance(v, str):
            return v.strip()
        return v


class ListTableFlowRegionsHandler(BaseToolHandler):
    """Handler for listing Tableflow regions in Confluent Cloud"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Retrieve a sorted, filtered, paginated list of all tableflow regions.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Optional arguments for filtering and pagination
            session_id: Optional session identifier
            
        Returns:
            Result containing the list of Tableflow regions
        """
        try:
            # Parse and validate arguments
            args = ListTableFlowRegionsArguments.model_validate(tool_arguments or {})
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
        
        try:
            # Build URL with query parameters
            # Note: The TypeScript version defines pageSize and pageToken but doesn't use them
            # Including them here for completeness
            url_parts = ["/tableflow/v1/regions"]
            query_params = []
            
            if args.cloud:
                query_params.append(f"cloud={args.cloud}")
            query_params.append(f"page_size={args.page_size}")
            query_params.append(f"page_token={args.page_token}")
            
            if query_params:
                url = f"{url_parts[0]}?{'&'.join(query_params)}"
            else:
                url = url_parts[0]
            
            response = await client.get(url)
            
            if response.status_code >= 400:
                error_text = await response.text()
                return self.create_response(
                    f"Failed to list Tableflow regions for {args.cloud or 'all clouds'}: {error_text}",
                    is_error=True
                )
            
            result = await response.json()
            return self.create_response(
                f"Tableflow Regions: {json.dumps(result, indent=2)}"
            )
            
        except Exception as e:
            return self.create_response(
                f"Error listing Tableflow regions: {str(e)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.LIST_TABLEFLOW_REGIONS,
            description="Retrieve a sorted, filtered, paginated list of all tableflow regions.",
            inputSchema=ListTableFlowRegionsArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["TABLEFLOW_API_KEY", "TABLEFLOW_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True