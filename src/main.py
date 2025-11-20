#!/usr/bin/env python3
"""
Main entry point for the MCP Confluent Server.
"""

import asyncio
import signal
import sys
from typing import Dict, Optional

from mcp.server import McpServer

from src.cli import (
    get_filtered_tool_names,
    get_package_version,
    parse_cli_args,
)
from confluent_cloud.client_manager import DefaultClientManager, ClientManagerConfig
from confluent_cloud.tools.base_tools import ToolHandler
from confluent_cloud.tools.tool_factory import ToolFactory
from confluent_cloud.tools.tool_name import ToolName
from env import init_env
from logger import logger, set_log_level
from mcp.transports import TransportManager


class Application:
    """Main application class for managing server lifecycle"""

    def __init__(self):
        self.transport_manager: Optional[TransportManager] = None
        self.client_manager: Optional[DefaultClientManager] = None
        self.server: Optional[McpServer] = None
        self.shutdown_event = asyncio.Event()

    async def cleanup(self) -> None:
        """Perform graceful shutdown of all components"""
        logger.info("Shutting down...")
        
        try:
            if self.transport_manager:
                await self.transport_manager.stop()
            
            if self.client_manager:
                await self.client_manager.disconnect()
            
            if self.server:
                await self.server.close()
                
            logger.info("Shutdown complete")
        except Exception as error:
            logger.error(f"Error during cleanup: {error}")
        finally:
            self.shutdown_event.set()

    def handle_signal(self, signum: int) -> None:
        """Handle shutdown signals"""
        signal_name = signal.Signals(signum).name
        logger.info(f"Received signal {signal_name}, initiating shutdown...")
        asyncio.create_task(self.cleanup())

    async def run(self) -> None:
        """Main application logic"""
        try:
            # Parse command line arguments
            cli_options = parse_cli_args()
            
            # Initialize environment
            env = await init_env()
            set_log_level(env.LOG_LEVEL)

            # Build Kafka client configuration
            kafka_client_config = {
                "bootstrap.servers": env.BOOTSTRAP_SERVERS,
                "client.id": "mcp-confluent",
            }

            # Add authentication if credentials are provided
            if env.KAFKA_API_KEY and env.KAFKA_API_SECRET:
                kafka_client_config.update({
                    "security.protocol": "sasl_ssl",
                    "sasl.mechanisms": "PLAIN",
                    "sasl.username": env.KAFKA_API_KEY,
                    "sasl.password": env.KAFKA_API_SECRET,
                })

            # Merge CLI config
            if cli_options.kafka_config:
                kafka_client_config.update(cli_options.kafka_config)

            # Create client manager configuration
            client_config = ClientManagerConfig(
                kafka=kafka_client_config,
                endpoints={
                    "cloud": env.CONFLUENT_CLOUD_REST_ENDPOINT,
                    "flink": env.FLINK_REST_ENDPOINT,
                    "schemaRegistry": env.SCHEMA_REGISTRY_ENDPOINT,
                    "kafka": env.KAFKA_REST_ENDPOINT,
                },
                auth={
                    "cloud": {
                        "apiKey": env.CONFLUENT_CLOUD_API_KEY,
                        "apiSecret": env.CONFLUENT_CLOUD_API_SECRET,
                    },
                    "tableflow": {
                        "apiKey": env.TABLEFLOW_API_KEY,
                        "apiSecret": env.TABLEFLOW_API_SECRET,
                    },
                    "flink": {
                        "apiKey": env.FLINK_API_KEY,
                        "apiSecret": env.FLINK_API_SECRET,
                    },
                    "schemaRegistry": {
                        "apiKey": env.SCHEMA_REGISTRY_API_KEY,
                        "apiSecret": env.SCHEMA_REGISTRY_API_SECRET,
                    },
                    "kafka": {
                        "apiKey": env.KAFKA_API_KEY,
                        "apiSecret": env.KAFKA_API_SECRET,
                    },
                },
            )

            self.client_manager = DefaultClientManager(client_config)

            # Get filtered tool names
            filtered_tool_names = get_filtered_tool_names(cli_options)

            # Handle --list-tools option
            if cli_options.list_tools:
                self._list_tools(filtered_tool_names)
                sys.exit(0)

            # Initialize and filter tools
            tool_handlers = self._initialize_tools(
                filtered_tool_names,
                cli_options.disable_confluent_cloud_tools,
                env,
            )

            # Create MCP server
            self.server = McpServer(
                name="confluent",
                version=get_package_version(),
            )

            # Register tools
            self._register_tools(tool_handlers)

            # Create and start transport manager
            self.transport_manager = TransportManager(self.server)

            logger.info(f"Starting transports: {', '.join(cli_options.transports)}")
            await self.transport_manager.start(
                cli_options.transports,
                env.HTTP_PORT,
                env.HTTP_HOST,
                env.HTTP_MCP_ENDPOINT_PATH,
                env.SSE_MCP_ENDPOINT_PATH,
                env.SSE_MCP_MESSAGE_ENDPOINT_PATH,
            )

            # Set up signal handlers
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGQUIT):
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: self.handle_signal(s)
                )

            logger.info("Server started successfully")
            
            # Wait for shutdown signal
            await self.shutdown_event.wait()

        except Exception as error:
            logger.error({"err": str(error)}, "Error starting server")
            raise

    def _list_tools(self, filtered_tool_names) -> None:
        """Print available tools and their descriptions"""
        MAX_DESC_LENGTH = 120
        for tool_name in filtered_tool_names:
            config = ToolFactory.get_tool_config(tool_name)
            desc = " ".join(config.description.split()).strip()
            if len(desc) > MAX_DESC_LENGTH:
                desc = desc[:MAX_DESC_LENGTH - 3] + "..."
            print(f"\x1b[32m{config.name}\x1b[0m: {desc}")

    def _initialize_tools(
        self,
        filtered_tool_names,
        disable_confluent_cloud_tools: bool,
        env,
    ) -> Dict[ToolName, ToolHandler]:
        """Initialize and filter tools based on requirements"""
        tool_handlers: Dict[ToolName, ToolHandler] = {}

        for tool_name in ToolName:
            if tool_name not in filtered_tool_names:
                logger.warn(
                    f"Tool {tool_name} disabled due to allow/block list rules"
                )
                continue

            handler = ToolFactory.create_tool_handler(tool_name)

            # Skip cloud-only tools if disabled
            if disable_confluent_cloud_tools and handler.is_confluent_cloud_only():
                logger.warn(
                    f"Tool {tool_name} disabled due to --disable-confluent-cloud-tools"
                )
                continue

            # Check required environment variables
            missing_vars = [
                var_name
                for var_name in handler.get_required_env_vars()
                if not getattr(env, var_name, None)
            ]

            if not missing_vars:
                tool_handlers[tool_name] = handler
                logger.info(f"Tool {tool_name} enabled")
            else:
                logger.warn(
                    f"Tool {tool_name} disabled due to missing environment "
                    f"variables: {', '.join(missing_vars)}"
                )

        return tool_handlers

    def _register_tools(self, tool_handlers: Dict[ToolName, ToolHandler]) -> None:
        """Register tools with the MCP server"""
        for tool_name, handler in tool_handlers.items():
            config = handler.get_tool_config()

            # Create closure to capture handler
            async def create_tool_handler(h=handler):
                async def tool_func(args, context=None):
                    session_id = context.session_id if context else None
                    return await h.handle(self.client_manager, args, session_id)
                return tool_func

            self.server.tool(
                str(tool_name),
                config.description,
                config.input_schema,
                asyncio.run(create_tool_handler()),
            )


async def main() -> None:
    """Main entry point"""
    app = Application()
    try:
        await app.run()
    except Exception as error:
        logger.error({"error": str(error)}, "Fatal error")
        await app.cleanup()
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
        sys.exit(0)
    except Exception as error:
        logger.error({"error": str(error)}, "Fatal error")
        sys.exit(1)