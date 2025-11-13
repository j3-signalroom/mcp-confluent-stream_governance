from abc import ABC, abstractmethod
from typing import Any, Protocol, runtime_checkable
from pydantic import BaseModel, Field


class ToolContent(BaseModel):
    """Content item in a tool response"""
    type: str = "text"
    text: str


class CallToolResult(BaseModel):
    """Result from a tool call"""
    content: list[ToolContent]
    isError: bool = False
    _meta: dict[str, Any] | None = Field(default=None, alias="_meta")

    class Config:
        populate_by_name = True  # Allow both _meta and alias


class ToolConfig(BaseModel):
    """Configuration for a tool"""
    name: str
    description: str
    inputSchema: dict[str, Any]  # Zod schema equivalent


@runtime_checkable
class ToolHandler(Protocol):
    """Protocol (interface) for tool handlers"""
    
    def handle(
        self,
        client_manager: Any,  # Replace with your ClientManager type
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """Handle the tool execution"""
        ...
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        ...
    
    def get_required_env_vars(self) -> list[str]:
        """
        Returns an array of environment variables required for this tool to function.
        
        This method is used to conditionally enable/disable tools based on the availability
        of required environment variables. Tools will be disabled if any of their required
        environment variables are not set.
        
        Example:
            return [
                "KAFKA_API_KEY",
                "KAFKA_API_SECRET",
                "BOOTSTRAP_SERVERS"
            ]
        
        Returns:
            Array of environment variable names required by this tool
        """
        ...
    
    def is_confluent_cloud_only(self) -> bool:
        """
        Returns True if this tool can only be used with Confluent Cloud REST APIs.
        Override in subclasses for cloud-only tools.
        """
        ...


class BaseToolHandler(ABC):
    """Abstract base class for tool handlers"""
    
    @abstractmethod
    def handle(
        self,
        client_manager: Any,  # Replace with your ClientManager type
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """Handle the tool execution"""
        pass
    
    @abstractmethod
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        pass
    
    def get_required_env_vars(self) -> list[str]:
        """
        Default implementation that returns an empty list, indicating no environment
        variables are required. Override this method in your tool handler if the tool
        requires specific environment variables to function.
        
        Returns:
            Empty list by default
        """
        return []
    
    def is_confluent_cloud_only(self) -> bool:
        """
        Default implementation returns False, indicating the tool is not Confluent Cloud only.
        Override in subclasses for cloud-only tools.
        """
        return False
    
    def create_response(
        self,
        message: str,
        is_error: bool = False,
        meta: dict[str, Any] | None = None,
    ) -> CallToolResult:
        """Create a standardized tool response"""
        return CallToolResult(
            content=[
                ToolContent(
                    type="text",
                    text=message
                )
            ],
            isError=is_error,
            _meta=meta
        )