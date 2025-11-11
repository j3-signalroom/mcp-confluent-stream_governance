from abc import ABC, abstractmethod
from enum import Enum
from pydantic import BaseModel, Field


class TransportType(str, Enum):
    """Supported transport types for MCP server"""
    HTTP = "http"
    SSE = "sse"
    STDIO = "stdio"


class Transport(ABC):
    """Abstract base class for MCP transports"""
    
    @abstractmethod
    async def connect(self) -> None:
        """Establish the transport connection"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Close the transport connection and clean up resources"""
        pass


class ServerConfig(BaseModel):
    """HTTP server configuration"""
    port: int = Field(..., gt=0, le=65535, description="Server port number")
    host: str = Field(..., description="Server host address")
    
    class Config:
        frozen = False