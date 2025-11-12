import asyncio
from typing import Optional
import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import os
from mcp.transports.types import ServerConfig

from utilities import setup_logging


# Setup module logging
logger = setup_logging()

class HttpServer:
    def __init__(self):
        self._app: Optional[FastAPI] = None
        self._server: Optional[uvicorn.Server] = None
        self._is_swagger_configured = False
        self._server_task: Optional[asyncio.Task] = None

    async def prepare(self) -> None:
        """Prepare the FastAPI application with OpenAPI/Swagger configuration"""
        if self._is_swagger_configured:
            return
        
        # Load environment variables from .env file
        load_dotenv()

        # Create FastAPI instance with OpenAPI configuration
        self._app = FastAPI(
            title="Confluent MCP Server API",
            description="API documentation for the Confluent MCP Server",
            version="1.0.0",
            servers=[
                {
                    "url": f"http://{os.getenv('HTTP_HOST')}:{os.getenv('HTTP_PORT')}",
                    "description": "Local development server",
                }
            ],
            openapi_tags=[
                {"name": "mcp", "description": "Model Context Protocol endpoints"}
            ],
            docs_url="/documentation",  # Swagger UI path
            redoc_url="/redoc",  # ReDoc path (alternative docs)
            openapi_url="/openapi.json",  # OpenAPI schema path
            # Configure Swagger UI
            swagger_ui_parameters={
                "docExpansion": "list",
                "deepLinking": True,
            },
        )

        # Add security scheme to OpenAPI schema
        self._configure_security_schemes()

        self._is_swagger_configured = True
        logger.info("FastAPI application prepared with OpenAPI documentation")

    def _configure_security_schemes(self) -> None:
        """Configure OpenAPI security schemes"""
        if self._app:
            # Customize OpenAPI schema to add security schemes
            def custom_openapi():
                if self._app.openapi_schema:
                    return self._app.openapi_schema

                openapi_schema = self._app.openapi()
                
                # Add security schemes
                if "components" not in openapi_schema:
                    openapi_schema["components"] = {}
                
                openapi_schema["components"]["securitySchemes"] = {
                    "apiKey": {
                        "type": "apiKey",
                        "name": "X-API-Key",
                        "in": "header",
                    }
                }
                
                self._app.openapi_schema = openapi_schema
                return openapi_schema

            self._app.openapi = custom_openapi

    def get_instance(self) -> FastAPI:
        """Get the FastAPI application instance"""
        if not self._app:
            raise RuntimeError("HttpServer not prepared. Call prepare() first.")
        return self._app

    async def start(self, config: ServerConfig) -> None:
        """Start the HTTP server"""
        try:
            if not self._app:
                raise RuntimeError("HttpServer not prepared. Call prepare() first.")

            # Configure uvicorn server
            uvicorn_config = uvicorn.Config(
                app=self._app,
                host=config.host,
                port=config.port,
                log_config=None,  # Use our custom logger
                access_log=False,  # Disable uvicorn access logs (use custom logger)
            )

            self._server = uvicorn.Server(uvicorn_config)

            logger.info(
                f"Starting server on http://{config.host}:{config.port}"
            )
            logger.info(
                f"API documentation available at http://{config.host}:{config.port}/documentation"
            )

            # Run server in the current event loop
            await self._server.serve()

        except Exception as error:
            logger.error({"error": str(error)}, "Failed to start server")
            raise

    async def stop(self) -> None:
        """Stop the HTTP server gracefully"""
        try:
            logger.info("Shutting down server...")
            
            if self._server:
                self._server.should_exit = True
                
                # Wait a bit for graceful shutdown
                await asyncio.sleep(0.1)
                
            logger.info("Server shut down successfully")

        except Exception as error:
            logger.error({"error": str(error)}, "Error while shutting down server")
            raise


# Alternative implementation with lifespan context manager (FastAPI best practice)
class HttpServerWithLifespan:
    def __init__(self):
        self._is_swagger_configured = False
        self._server: Optional[uvicorn.Server] = None
        
        @asynccontextmanager
        async def lifespan(app: FastAPI):
            # Startup
            logger.info("Server starting up...")
            yield
            # Shutdown
            logger.info("Server shutting down...")

        self._app = FastAPI(
            title="Confluent MCP Server API",
            description="API documentation for the Confluent MCP Server",
            version="1.0.0",
            lifespan=lifespan,
            docs_url="/documentation",
            swagger_ui_parameters={
                "docExpansion": "list",
                "deepLinking": True,
            },
        )

    async def prepare(self) -> None:
        """Prepare the server (configures OpenAPI if not already done)"""
        if self._is_swagger_configured:
            return

        # Configure servers dynamically
        self._app.servers = [
            {
                "url": f"http://{os.getenv('HTTP_HOST')}:{os.getenv('HTTP_PORT')}",
                "description": "Local development server",
            }
        ]

        # Add OpenAPI tags
        self._app.openapi_tags = [
            {"name": "mcp", "description": "Model Context Protocol endpoints"}
        ]

        self._configure_security_schemes()
        self._is_swagger_configured = True
        logger.info("FastAPI application prepared")

    def _configure_security_schemes(self) -> None:
        """Configure OpenAPI security schemes"""
        def custom_openapi():
            if self._app.openapi_schema:
                return self._app.openapi_schema

            openapi_schema = self._app.openapi()
            
            if "components" not in openapi_schema:
                openapi_schema["components"] = {}
            
            openapi_schema["components"]["securitySchemes"] = {
                "apiKey": {
                    "type": "apiKey",
                    "name": "X-API-Key",
                    "in": "header",
                }
            }
            
            self._app.openapi_schema = openapi_schema
            return openapi_schema

        self._app.openapi = custom_openapi

    def get_instance(self) -> FastAPI:
        """Get the FastAPI application instance"""
        return self._app

    async def start(self, config: ServerConfig) -> None:
        """Start the HTTP server"""
        try:
            uvicorn_config = uvicorn.Config(
                app=self._app,
                host=config.host,
                port=config.port,
                log_config=None,
                access_log=False,
            )

            self._server = uvicorn.Server(uvicorn_config)

            logger.info(f"Starting server on http://{config.host}:{config.port}")
            logger.info(
                f"API documentation: http://{config.host}:{config.port}/documentation"
            )

            await self._server.serve()

        except Exception as error:
            logger.error({"error": str(error)}, "Failed to start server")
            raise

    async def stop(self) -> None:
        """Stop the HTTP server gracefully"""
        try:
            logger.info("Shutting down server...")
            
            if self._server:
                self._server.should_exit = True
                await asyncio.sleep(0.1)
                
            logger.info("Server shut down successfully")

        except Exception as error:
            logger.error({"error": str(error)}, "Error while shutting down server")
            raise