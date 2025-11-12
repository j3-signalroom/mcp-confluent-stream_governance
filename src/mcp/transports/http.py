from typing import Dict, Optional
from uuid import uuid4
from fastapi import Header, Request, status
from fastapi.responses import JSONResponse
from mcp.server import McpServer
from mcp.server.streamable_http import StreamableHTTPServerTransport
from mcp.transports.ping import ping_handler, PingRequest, PingResponse
from mcp.transports.server import HttpServer
from mcp.transports.types import Transport
from pydantic import BaseModel

from utilities import setup_logging


# Setup module logging
logger = setup_logging()

class McpSession(BaseModel):
    sessionId: str


class McpError(BaseModel):
    error: str


class HttpTransport(Transport):
    def __init__(
        self,
        server: McpServer,
        http_server: HttpServer,
        http_mcp_endpoint_path: str = "/mcp",
    ):
        self.server = server
        self.http_server = http_server
        self.http_mcp_endpoint_path = http_mcp_endpoint_path
        self.sessions: Dict[str, StreamableHTTPServerTransport] = {}

    async def connect(self) -> None:
        """Register HTTP transport routes"""
        app = self.http_server.get_instance()

        # POST handler for new sessions and message sending
        @app.post(
            self.http_mcp_endpoint_path,
            tags=["mcp"],
            summary="Create a new MCP session or send a message",
            response_model=McpSession,
            responses={404: {"model": McpError}},
        )
        async def post_mcp(
            request: Request,
            mcp_session_id: Optional[str] = Header(None, alias="mcp-session-id"),
        ) -> JSONResponse:
            transport: StreamableHTTPServerTransport

            if mcp_session_id and mcp_session_id in self.sessions:
                transport = self.sessions[mcp_session_id]
            else:
                def on_session_initialized(sid: str) -> None:
                    self.sessions[sid] = transport

                transport = StreamableHTTPServerTransport(
                    session_id_generator=lambda: str(uuid4()),
                    on_session_initialized=on_session_initialized,
                )

                await self.server.connect(transport)

            body = await request.body()
            await transport.handle_request(request, body)
            return JSONResponse(content={"sessionId": transport.session_id})

        # GET handler for session status and message receiving
        @app.get(
            self.http_mcp_endpoint_path,
            tags=["mcp"],
            summary="Get session status or receive messages",
            response_model=McpSession,
            responses={404: {"model": McpError}},
        )
        async def get_mcp(
            request: Request,
            mcp_session_id: str = Header(..., alias="mcp-session-id"),
        ) -> JSONResponse:
            if not mcp_session_id or mcp_session_id not in self.sessions:
                return JSONResponse(
                    status_code=status.HTTP_404_NOT_FOUND,
                    content={"error": "Session not found"},
                )

            transport = self.sessions[mcp_session_id]
            await transport.handle_request(request)
            return JSONResponse(content={"sessionId": transport.session_id})

        # DELETE handler for session cleanup
        @app.delete(
            self.http_mcp_endpoint_path,
            tags=["mcp"],
            summary="Delete an MCP session",
            response_model=McpSession,
            responses={404: {"model": McpError}},
        )
        async def delete_mcp(
            request: Request,
            mcp_session_id: str = Header(..., alias="mcp-session-id"),
        ) -> JSONResponse:
            if not mcp_session_id or mcp_session_id not in self.sessions:
                return JSONResponse(
                    status_code=status.HTTP_404_NOT_FOUND,
                    content={"error": "Session not found"},
                )

            transport = self.sessions[mcp_session_id]
            await transport.handle_request(request)
            
            # Clean up the session
            del self.sessions[mcp_session_id]
            
            return JSONResponse(content={"sessionId": mcp_session_id})

        # POST ping endpoint for health checks
        @app.post(
            "/ping",
            tags=["mcp"],
            summary="JSON-RPC 2.0 ping endpoint",
            response_model=PingResponse,
        )
        async def post_ping(request: PingRequest) -> PingResponse:
            return await ping_handler(request)

        logger.info("HTTP transport routes registered")

    async def disconnect(self) -> None:
        """Clean up all active sessions"""
        logger.info("Cleaning up HTTP transport sessions...")
        
        for session_id, transport in list(self.sessions.items()):
            if session_id:
                del self.sessions[session_id]
            await transport.close()
