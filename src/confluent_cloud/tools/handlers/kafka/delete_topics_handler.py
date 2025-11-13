from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator
from mcp.types import CallToolResult

from client_manager import ClientManager
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class DeleteKafkaTopicsArguments(BaseModel):
    """Arguments for deleting Kafka topics."""
    
    topic_names: List[str] = Field(
        ...,
        min_length=1,
        description="Names of kafka topics to delete",
    )

    @field_validator("topic_names")
    @classmethod
    def validate_topic_names(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("topic_names cannot be empty")
        return v


class DeleteTopicsHandler(BaseToolHandler):
    """Handler for deleting Kafka topics."""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: Dict[str, Any],
        session_id: Optional[str] = None,
    ) -> CallToolResult:
        """
        Deletes one or more Kafka topics.
        
        Args:
            client_manager: The client manager for Kafka admin operations
            tool_arguments: The arguments containing topic names to delete
            session_id: Optional session ID (unused for this operation)
            
        Returns:
            A CallToolResult indicating success or failure
        """
        # Parse and validate arguments
        args = DeleteKafkaTopicsArguments(**tool_arguments)
        
        # Get admin client and delete topics
        admin_client = await client_manager.get_admin_client()
        await admin_client.delete_topics(topics=args.topic_names)
        
        return self.create_response(
            f"Deleted Kafka topics: {', '.join(args.topic_names)}",
            is_error=False,
        )

    def get_tool_config(self) -> ToolConfig:
        """Returns the tool configuration."""
        return ToolConfig(
            name=ToolName.DELETE_TOPICS,
            description="Delete the topic with the given names.",
            input_schema=DeleteKafkaTopicsArguments.model_json_schema(),
        )

    def get_required_env_vars(self) -> List[str]:
        """Returns the required environment variables."""
        return ["KAFKA_API_KEY", "KAFKA_API_SECRET", "BOOTSTRAP_SERVERS"]