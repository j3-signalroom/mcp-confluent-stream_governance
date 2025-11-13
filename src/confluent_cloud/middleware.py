"""
Middleware and authentication with Pydantic validation.
"""

import base64
from typing import Optional
import httpx
from pydantic import BaseModel, Field, SecretStr

from logger import logger


class ConfluentEndpoints(BaseModel):
    """Configuration for Confluent service endpoints"""
    cloud: Optional[str] = None
    tableflow: Optional[str] = None
    flink: Optional[str] = None
    schema_registry: Optional[str] = Field(default=None, alias="schemaRegistry")
    kafka: Optional[str] = None
    
    model_config = {"populate_by_name": True}


class ConfluentAuth(BaseModel):
    """Authentication credentials for Confluent services"""
    api_key: str = Field(..., min_length=1, description="API key for authentication")
    api_secret: SecretStr = Field(..., description="API secret for authentication")
    
    def get_basic_auth_header(self) -> str:
        """
        Generate Basic Authentication header value.
        
        Returns:
            Base64-encoded Basic auth header value
        """
        credentials = f"{self.api_key}:{self.api_secret.get_secret_value()}"
        encoded = base64.b64encode(credentials.encode()).decode('ascii')
        return f"Basic {encoded}"


class ConfluentAuthMiddleware(httpx.Auth):
    """httpx Auth class for Confluent services"""
    
    def __init__(self, auth: ConfluentAuth):
        self.auth = auth
        self._auth_header = auth.get_basic_auth_header()
    
    def auth_flow(self, request: httpx.Request):
        """Add authorization header to request"""
        logger.debug({"request": str(request)}, "Processing request")
        request.headers["Authorization"] = self._auth_header
        yield request


def create_auth_middleware(auth: ConfluentAuth) -> ConfluentAuthMiddleware:
    """
    Create an authentication middleware for httpx clients.
    
    Args:
        auth: ConfluentAuth object with API key and secret
        
    Returns:
        ConfluentAuthMiddleware instance
    """
    return ConfluentAuthMiddleware(auth)