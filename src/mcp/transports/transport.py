import logging
from typing import Dict, List, Optional
import asyncio
from enum import Enum

from mcp.server import McpServer

# Assuming these imports exist in your Python project
from src.logger import logger
from mcp.transports.http import HttpTransport
from mcp.transports.server import HttpServer
from mcp.transports.sse import SseTransport
from mcp.transports.stdio import StdioTransport
from mcp.transports.types import Transport, TransportType


class TransportManager:
    def __init__(self, server: McpServer):
        self.server = server
        self.transports: Dict[TransportType, Transport] = {}
        self.http_server: Optional[HttpServer] = None

    async def start(
        self,
        types: List[TransportType],
        port: Optional[int] = None,
        host: Optional[str] = None,
        http_mcp_endpoint_path: Optional[str] = None,
        sse_mcp_endpoint_path: Optional[str] = None,
        sse_mcp_message_endpoint_path: Optional[str] = None,
    ) -> None:
        """Start all specified transport types"""
        try:
            needs_http_server = any(
                t in [TransportType.HTTP, TransportType.SSE] for t in types
            )

            # Initialize and prepare HTTP server if needed
            if needs_http_server:
                self.http_server = HttpServer()
                await self.http_server.prepare()

            # Create and connect all transports
            async def create_and_connect(transport_type: TransportType) -> None:
                transport = await self._create_transport(
                    transport_type,
                    http_mcp_endpoint_path,
                    sse_mcp_endpoint_path,
                    sse_mcp_message_endpoint_path,
                )
                self.transports[transport_type] = transport
                await transport.connect()

            await asyncio.gather(
                *[create_and_connect(t) for t in types]
            )

            # Start HTTP server if needed (after routes are registered)
            if needs_http_server and self.http_server:
                if port and host:
                    await self.http_server.start(port=port, host=host)
                else:
                    logger.error("Port and host are required")
                    raise ValueError("Port and host are required")

            logger.info("All transports started successfully")

        except Exception as error:
            logger.error({"error": str(error)}, "Failed to start transports")
            # Clean up any partially started transports
            await self.stop()
            raise

    async def stop(self) -> None:
        """Stop all transports and clean up resources"""
        # Stop HTTP server first to prevent new requests
        if self.http_server:
            await self.http_server.stop()

        # Then disconnect all transports
        async def disconnect_transport(transport: Transport) -> None:
            try:
                await transport.disconnect()
            except Exception as error:
                logger.error(
                    {"error": str(error)},
                    f"Failed to disconnect transport: {transport.__class__.__name__}",
                )

        await asyncio.gather(
            *[disconnect_transport(t) for t in self.transports.values()],
            return_exceptions=True,
        )

        self.transports.clear()
        self.http_server = None

    async def _create_transport(
        self,
        transport_type: TransportType,
        http_mcp_endpoint_path: Optional[str] = None,
        sse_mcp_endpoint_path: Optional[str] = None,
        sse_mcp_message_endpoint_path: Optional[str] = None,
    ) -> Transport:
        """Factory method to create transport instances"""
        if transport_type == TransportType.HTTP:
            if not self.http_server:
                raise RuntimeError("HTTP server not initialized")
            return HttpTransport(
                self.server,
                self.http_server,
                http_mcp_endpoint_path,
            )

        elif transport_type == TransportType.SSE:
            if not self.http_server:
                raise RuntimeError("HTTP server not initialized")
            return SseTransport(
                self.server,
                self.http_server,
                sse_mcp_endpoint_path,
                sse_mcp_message_endpoint_path,
            )

        elif transport_type == TransportType.STDIO:
            return StdioTransport(self.server)

        else:
            raise ValueError(f"Unsupported transport type: {transport_type}")