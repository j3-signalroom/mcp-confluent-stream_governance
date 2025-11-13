from typing import Any, Dict, List, Optional
from mcp.types import CallToolResult
from pydantic import BaseModel

from client_manager import ClientManager
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class ListTopicArgs(BaseModel):
    """Arguments for listing Kafka topics (no arguments required)."""
    pass


class ListTopicsHandler(BaseToolHandler):
    """Handler for listing all Kafka topics in a cluster."""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: Dict[str, Any],
        session_id: Optional[str] = None,
    ) -> CallToolResult:
        """
        Lists all topics in the Kafka cluster.
        
        Args:
            client_manager: The client manager for Kafka admin operations
            tool_arguments: The arguments (unused for this operation)
            session_id: Optional session ID (unused for this operation)
            
        Returns:
            A CallToolResult containing the list of topics
        """
        # Get admin client and list topics
        admin_client = await client_manager.get_admin_client()
        topics = await admin_client.list_topics()
        
        return self.create_response(
            f"Kafka topics: {', '.join(topics)}",
            is_error=False,
        )

    def get_tool_config(self) -> ToolConfig:
        """Returns the tool configuration."""
        return ToolConfig(
            name=ToolName.LIST_TOPICS,
            description="List all topics in the Kafka cluster.",
            input_schema=ListTopicArgs.model_json_schema(),
        )

    def get_required_env_vars(self) -> List[str]:
        """Returns the required environment variables."""
        return ["KAFKA_API_KEY", "KAFKA_API_SECRET", "BOOTSTRAP_SERVERS"]