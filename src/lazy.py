"""
Lazy initialization classes with property-style access.
"""

from typing import TypeVar, Generic, Optional, Callable, Awaitable
import asyncio

from logger import logger


T = TypeVar("T")


class Lazy(Generic[T]):
    """Lazy initialization for synchronous values with property-style access."""

    def __init__(
        self,
        supplier: Callable[[], T],
        close_handler: Optional[Callable[[T], None]] = None,
    ):
        self._instance: Optional[T] = None
        self._supplier = supplier
        self._close_handler = close_handler

    @property
    def value(self) -> T:
        """Get the lazy-loaded value (property-style access)"""
        return self.get()

    def get(self) -> T:
        """Retrieve the lazy-loaded value, initializing if necessary"""
        if self._instance is None:
            try:
                self._instance = self._supplier()
                instance_type = (
                    self._instance.__class__.__name__
                    if hasattr(self._instance, "__class__")
                    else type(self._instance).__name__
                )
                logger.debug(f"Lazy instance created with type {instance_type}")
            except Exception as error:
                raise RuntimeError(f"Failed to initialize lazy instance: {error}") from error
        
        return self._instance

    def close(self) -> None:
        """Close and clean up the lazy-loaded instance"""
        if self._instance is not None:
            if self._close_handler is not None:
                logger.debug(
                    f"Initiating close handler for lazy instance of type {type(self._instance).__name__}"
                )
                try:
                    self._close_handler(self._instance)
                    logger.debug(
                        f"Lazy instance closed with type {type(self._instance).__name__}"
                    )
                except Exception as error:
                    logger.error(f"Error closing lazy instance: {error}")
            self._instance = None

    def is_initialized(self) -> bool:
        """Check if the value has been initialized"""
        return self._instance is not None

    def __enter__(self) -> T:
        """Context manager support"""
        return self.get()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup"""
        self.close()
        return False


class AsyncLazy(Generic[T]):
    """Lazy initialization for asynchronous values."""

    def __init__(
        self,
        async_factory: Callable[[], Awaitable[T]],
        cleanup: Optional[Callable[[T], Awaitable[None]]] = None,
    ):
        self._instance: Optional[T] = None
        self._async_factory = async_factory
        self._cleanup = cleanup
        self._initialization_task: Optional[asyncio.Task[T]] = None
        self._lock = asyncio.Lock()

    async def get(self) -> T:
        """Retrieve the lazy-loaded value, initializing if necessary"""
        if self._instance is not None:
            return self._instance

        async with self._lock:
            # Double-check pattern after acquiring lock
            if self._instance is not None:
                return self._instance

            # Start initialization if not already in progress
            if self._initialization_task is None:
                self._initialization_task = asyncio.create_task(self._async_factory())

            try:
                self._instance = await self._initialization_task
                instance_type = (
                    self._instance.__class__.__name__
                    if hasattr(self._instance, "__class__")
                    else type(self._instance).__name__
                )
                logger.debug(f"Async Lazy instance created with type {instance_type}")
                return self._instance
            except Exception as error:
                # Reset task on failure to allow retry
                self._initialization_task = None
                raise RuntimeError(
                    f"Failed to initialize async lazy instance: {error}"
                ) from error

    async def close(self) -> None:
        """Close and clean up the lazy-loaded instance"""
        async with self._lock:
            if self._instance is not None:
                if self._cleanup is not None:
                    logger.debug(
                        f"Initiating close handler for lazy instance of type {type(self._instance).__name__}"
                    )
                    try:
                        await self._cleanup(self._instance)
                        logger.debug(
                            f"Lazy instance closed with type {type(self._instance).__name__}"
                        )
                    except Exception as error:
                        logger.error(f"Error closing async lazy instance: {error}")
                
                self._instance = None
                self._initialization_task = None

    def is_initialized(self) -> bool:
        """Check if the value has been initialized"""
        return self._instance is not None

    async def __aenter__(self) -> T:
        """Async context manager support"""
        return await self.get()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager cleanup"""
        await self.close()
        return False