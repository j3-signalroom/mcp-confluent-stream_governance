"""
Environment variable management with validation.
"""

import os
import sys
from typing import Optional, Any
from pydantic import ValidationError

from src.env_schema import CombinedSchema
from logger import logger


# Type alias
Environment = CombinedSchema

# Module-level state
_is_initialized = False
_env_values: Optional[Environment] = None


def load_env() -> Environment:
    """
    Load and validate environment variables.
    
    Returns:
        Validated environment object
    """
    try:
        return CombinedSchema(**os.environ)
    except ValidationError as error:
        logger.error({"error": str(error)}, "Environment validation error")
        sys.exit(1)


def init_env() -> Environment:
    """
    Initialize environment and save the values.
    
    Returns:
        Validated environment configuration
    """
    global _is_initialized, _env_values
    
    if not _is_initialized:
        _env_values = load_env()
        _is_initialized = True
    
    return _env_values


class _EnvProxy:
    """Proxy for accessing environment variables"""
    
    def __getattr__(self, name: str) -> Any:
        if not _is_initialized or _env_values is None:
            raise RuntimeError(
                f"Environment not initialized. Attempted to access {name} "
                f"before initialization."
            )
        return getattr(_env_values, name)
    
    def __getitem__(self, name: str) -> Any:
        return self.__getattr__(name)


# Create singleton instance
env = _EnvProxy()