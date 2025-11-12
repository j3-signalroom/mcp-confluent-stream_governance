from typing import Literal
from pydantic import BaseModel, Field


class PingRequest(BaseModel):
    """JSON-RPC 2.0 Ping Request Schema"""
    jsonrpc: Literal["2.0"] = Field(..., description="JSON-RPC version")
    id: str = Field(..., description="Request ID")
    method: Literal["ping"] = Field(..., description="Method name")


class PingResult(BaseModel):
    """Empty result object for ping response"""
    pass


class PingResponse(BaseModel):
    """JSON-RPC 2.0 Ping Response Schema"""
    jsonrpc: Literal["2.0"] = Field(default="2.0", description="JSON-RPC version")
    id: str = Field(..., description="Request ID matching the request")
    result: PingResult = Field(default_factory=PingResult, description="Empty result object")


# JSON Schema equivalents (for OpenAPI documentation)
ping_request_schema = {
    "type": "object",
    "properties": {
        "jsonrpc": {"type": "string", "enum": ["2.0"]},
        "id": {"type": "string"},
        "method": {"type": "string", "enum": ["ping"]},
    },
    "required": ["jsonrpc", "id", "method"],
}

ping_response_schema = {
    "type": "object",
    "properties": {
        "jsonrpc": {"type": "string"},
        "id": {"type": "string"},
        "result": {"type": "object", "additionalProperties": False},
    },
    "required": ["jsonrpc", "id", "result"],
}


def ping_handler():
    """Factory function that returns a ping handler"""
    async def handler(request: PingRequest) -> PingResponse:
        """Handle JSON-RPC 2.0 ping requests"""
        return PingResponse(
            jsonrpc="2.0",
            id=request.id,
            result=PingResult(),
        )
    
    return handler


# Alternative: Direct handler function (more Pythonic)
async def ping_handler_direct(request: PingRequest) -> PingResponse:
    """Handle JSON-RPC 2.0 ping requests"""
    return PingResponse(
        jsonrpc="2.0",
        id=request.id,
        result=PingResult(),
    )