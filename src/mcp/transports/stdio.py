from mcp.server import McpServer
from mcp.server.stdio import StdioServerTransport
from mcp.transports.types import Transport

from utilities import setup_logging


# Setup module logging
logger = setup_logging()


class StdioTransport(Transport):
    """STDIO transport for MCP - simplest transport type"""

    def __init__(self, server: McpServer):
        self.server = server

    async def connect(self) -> None:
        """Connect STDIO transport"""
        await self.server.connect(StdioServerTransport())
        logger.info("STDIO transport connected")

    async def disconnect(self) -> None:
        """No cleanup needed for STDIO"""
        pass