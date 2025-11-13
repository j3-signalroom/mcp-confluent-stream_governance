from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field, field_validator
from mcp.types import CallToolResult

from client_manager import ClientManager
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName



class CreateTopicArgs(BaseModel):
    """Arguments for creating Kafka topics."""
    
    topic_names: List[str] = Field(
        ...,
        min_length=1,
        description="Names of kafka topics to create",
    )

    @field_validator("topic_names")
    @classmethod
    def validate_topic_names(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("topic_names cannot be empty")
        return v


class CreateTopicsHandler(BaseToolHandler):
    """Handler for creating Kafka topics."""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: Dict[str, Any],
        session_id: Optional[str] = None,
    ) -> CallToolResult:
        """
        Creates one or more Kafka topics.
        
        Args:
            client_manager: The client manager for Kafka admin operations
            tool_arguments: The arguments containing topic names to create
            session_id: Optional session ID (unused for this operation)
            
        Returns:
            A CallToolResult indicating success or failure
        """
        # Parse and validate arguments
        args = CreateTopicArgs(**tool_arguments)
        
        # Get admin client and create topics
        admin_client = await client_manager.get_admin_client()
        topics = [{"topic": name} for name in args.topic_names]
        success = await admin_client.create_topics(topics=topics)
        
        if not success:
            return self.create_response(
                f"Failed to create Kafka topics: {', '.join(args.topic_names)}",
                is_error=True,
            )
        
        return self.create_response(
            f"Created Kafka topics: {', '.join(args.topic_names)}",
            is_error=False,
        )

    def get_tool_config(self) -> ToolConfig:
        """Returns the tool configuration."""
        return ToolConfig(
            name=ToolName.CREATE_TOPICS,
            description="Create one or more Kafka topics.",
            input_schema=CreateTopicArgs.model_json_schema(),
        )

    def get_required_env_vars(self) -> List[str]:
        """Returns the required environment variables."""
        return ["KAFKA_API_KEY", "KAFKA_API_SECRET", "BOOTSTRAP_SERVERS"]