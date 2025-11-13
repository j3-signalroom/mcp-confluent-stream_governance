from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, HttpUrl
from mcp.types import CallToolResult

from client_manager import ClientManager
from confluent_cloud.tools.base_tools import BaseToolHandler, ToolConfig
from confluent_cloud.tools.tool_name import ToolName
from env_schema import EnvVar
from env import env


class TagAssignment(BaseModel):
    """Schema for a single tag assignment"""
    entity_type: str = Field(default="kafka_topic", alias="entityType")
    entity_name: str = Field(
        ...,
        alias="entityName",
        description=(
            f"Qualified name of the entity. If not provided, you can obtain it "
            f"from using the {ToolName.SEARCH_TOPICS_BY_NAME} tool. "
            f'example: "lsrc-g2p81:lkc-xq8k7g:my-flights"'
        ),
    )
    type_name: str = Field(..., alias="typeName", description="Name of the tag to assign")

    class Config:
        populate_by_name = True  # Allow both snake_case and camelCase


class AddTagToTopicArguments(BaseModel):
    """Arguments for adding tags to Kafka topics"""
    base_url: Optional[HttpUrl] = Field(
        default_factory=lambda: env.SCHEMA_REGISTRY_ENDPOINT or None,
        alias="baseUrl",
        description="The base URL of the Schema Registry REST API.",
    )
    tag_assignments: List[TagAssignment] = Field(
        ...,
        min_length=1,
        alias="tagAssignments",
        description="Array of tag assignments to create",
    )

    class Config:
        populate_by_name = True


class AddTagToTopicHandler(BaseToolHandler):
    """Handler for assigning tags to Kafka topics in Confluent Cloud"""

    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: Dict[str, Any],
    ) -> CallToolResult:
        """
        Handle the tag assignment request
        
        Args:
            client_manager: Confluent Cloud client manager
            tool_arguments: Tool arguments containing tag assignments
            
        Returns:
            CallToolResult with success or error message
        """
        # Parse and validate arguments
        args = AddTagToTopicArguments.model_validate(tool_arguments)

        # Update Schema Registry endpoint if provided
        if args.base_url is not None and str(args.base_url) != "":
            client_manager.set_confluent_cloud_schema_registry_endpoint(
                str(args.base_url)
            )

        # Get the Schema Registry REST client
        client = client_manager.get_confluent_cloud_schema_registry_rest_client()

        # Convert tag assignments to dict format for API
        tag_assignments_data = [
            assignment.model_dump(by_alias=True, exclude_none=True)
            for assignment in args.tag_assignments
        ]

        try:
            # Make the POST request to assign tags
            response = await client.post(
                "/catalog/v1/entity/tags",
                json=tag_assignments_data,
            )

            # Check for errors
            response.raise_for_status()
            
            # Get response data
            response_data = response.json()

            return self.create_response(
                f"Successfully assigned tag: {response_data}"
            )

        except Exception as error:
            return self.create_response(
                f"Failed to assign tag: {str(error)}",
                is_error=True,
            )

    def get_tool_config(self) -> ToolConfig:
        """
        Get the tool configuration for MCP
        
        Returns:
            ToolConfig with name, description, and input schema
        """
        return ToolConfig(
            name=ToolName.ADD_TAGS_TO_TOPIC,
            description="Assign existing tags to Kafka topics in Confluent Cloud.",
            input_schema=AddTagToTopicArguments.model_json_schema(),
        )

    def get_required_env_vars(self) -> List[EnvVar]:
        """
        Get required environment variables for this tool
        
        Returns:
            List of required environment variable names
        """
        return ["SCHEMA_REGISTRY_API_KEY", "SCHEMA_REGISTRY_API_SECRET"]

    def is_confluent_cloud_only(self) -> bool:
        """
        Check if this tool is only for Confluent Cloud
        
        Returns:
            True as this tool requires Confluent Cloud
        """
        return True