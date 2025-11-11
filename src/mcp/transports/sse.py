import logging
from typing import Dict, Any, Optional
from fastapi import Query, Request, Response, status
from fastapi.responses import JSONResponse
from sse_starlette.sse import EventSourceResponse
from pydantic import BaseModel

from mcp.server import McpServer
from mcp.server.sse import SSEServerTransport

# Assuming these imports exist in your Python project
from src.logger import logger
from src.mcp.transports.ping import (
    ping_handler,
    PingRequest,
    PingResponse,
)
from src.mcp.transports.server import HttpServer
from src.mcp.transports.types import Transport


class SseError(BaseModel):
    """SSE Error Response Schema"""
    error: str


class SseTransport(Transport):
    """Server-Sent Events transport for MCP"""

    def __init__(
        self,
        server: McpServer,
        http_server: HttpServer,
        sse_mcp_endpoint_path: str = "/sse",
        sse_mcp_message_endpoint_path: str = "/messages",
    ):
        self.server = server
        self.http_server = http_server
        self.sse_mcp_endpoint_path = sse_mcp_endpoint_path
        self.sse_mcp_message_endpoint_path = sse_mcp_message_endpoint_path
        self.sessions: Dict[str, SSEServerTransport] = {}

    async def connect(self) -> None:
        """Register SSE transport routes with the HTTP server"""
        app = self.http_server.get_instance()

        # SSE endpoint for establishing SSE connection
        @app.get(
            self.sse_mcp_endpoint_path,
            tags=["mcp"],
            summary="Establish SSE connection for real-time updates",
            responses={400: {"model": SseError}},
        )
        async def sse_connection_endpoint(request: Request):
            """Establish a new SSE connection for real-time updates"""
            
            async def event_generator():
                transport: Optional[SSEServerTransport] = None
                session_id: Optional[str] = None
                
                try:
                    # Create transport with the correct message endpoint
                    # Note: Python SSEServerTransport may have different constructor
                    transport = SSEServerTransport(
                        message_endpoint=self.sse_mcp_message_endpoint_path,
                    )

                    # Get the session ID from the transport
                    session_id = transport.session_id
                    if not session_id:
                        raise RuntimeError("Failed to get session ID from transport")

                    # Store the session before connecting
                    self.sessions[session_id] = transport

                    # Connect the transport to the server
                    await self.server.connect(transport)

                    logger.info(
                        f"New SSE connection established for session {session_id}"
                    )

                    # Stream events from the transport
                    async for event in transport.events():
                        # Check if client disconnected
                        if await request.is_disconnected():
                            break
                        yield event

                except Exception as error:
                    logger.error(
                        {"error": str(error)},
                        "Failed to establish SSE connection"
                    )
                    # Yield error event to client
                    yield {
                        "event": "error",
                        "data": '{"error": "Failed to establish SSE connection"}'
                    }

                finally:
                    # Cleanup on connection close
                    if session_id:
                        logger.info(
                            f"SSE connection closed for session {session_id}"
                        )
                        if session_id in self.sessions:
                            del self.sessions[session_id]
                    
                    if transport:
                        try:
                            transport.close()
                        except Exception as e:
                            logger.error(
                                {"error": str(e)},
                                f"Error closing transport for session {session_id}"
                            )

            return EventSourceResponse(
                event_generator(),
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",  # Disable nginx buffering
                },
            )

        # Message endpoint for receiving messages from clients
        @app.post(
            self.sse_mcp_message_endpoint_path,
            tags=["mcp"],
            summary="Send message to SSE connection",
            responses={400: {"model": SseError}},
        )
        async def sse_message_endpoint(
            request: Request,
            sessionId: str = Query(
                ...,
                description="SSE session ID",
                regex=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
            ),
        ) -> JSONResponse:
            """Receive and process messages for active SSE sessions"""
            try:
                logger.info(f"Received message for sessionId {sessionId}")

                transport = self.sessions.get(sessionId)

                if not transport:
                    logger.warn(f"No transport found for sessionId: {sessionId}")
                    return JSONResponse(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        content={"error": "No transport found for sessionId"},
                    )

                # Parse the request body
                body = await request.json() if request.headers.get("content-length") else {}

                # Handle the post message through the transport
                await transport.handle_post_message(body)

                return JSONResponse(
                    status_code=status.HTTP_200_OK,
                    content={"success": True},
                )

            except Exception as error:
                logger.error(
                    {"error": str(error)},
                    "Failed to process SSE message"
                )
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"error": "Failed to process message"},
                )

        # POST ping endpoint for health checks
        @app.post(
            "/ping",
            tags=["mcp"],
            summary="JSON-RPC 2.0 ping endpoint",
            response_model=PingResponse,
        )
        async def ping_endpoint(request: PingRequest) -> PingResponse:
            """Handle JSON-RPC 2.0 ping requests for health checks"""
            handler = ping_handler()
            return await handler(request)

        logger.info("SSE transport routes registered")

    async def disconnect(self) -> None:
        """Clean up all active SSE sessions"""
        logger.info("Cleaning up SSE transport sessions...")
        
        # Clean up all active sessions
        for session_id, transport in list(self.sessions.items()):
            try:
                if session_id and session_id in self.sessions:
                    del self.sessions[session_id]
                transport.close()
            except Exception as error:
                logger.error(
                    {"error": str(error)},
                    f"Error closing transport for session {session_id}",
                )
        
        self.sessions.clear()