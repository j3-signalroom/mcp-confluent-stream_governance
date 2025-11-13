"""
Client management functionality for Kafka and Confluent Cloud services.
"""
from typing import Optional, Dict, Any, Protocol
import httpx
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient

from middleware import (
    ConfluentAuth,
    ConfluentEndpoints,
    create_auth_middleware,
)
from lazy import Lazy, AsyncLazy

from logger import logger

class KafkaClientManager(Protocol):
    """Interface for managing Kafka client connections and operations."""

    def get_kafka_client(self) -> Any:
        """Gets the main Kafka client instance"""
        ...

    async def get_admin_client(self) -> AdminClient:
        """Gets a connected admin client for Kafka administration operations"""
        ...

    async def get_producer(self) -> Producer:
        """Gets a connected producer client for publishing messages"""
        ...

    async def get_consumer(self, session_id: Optional[str] = None) -> Consumer:
        """Gets a connected consumer client for subscribing to topics"""
        ...

    async def disconnect(self) -> None:
        """Disconnects and cleans up all client connections"""
        ...


class ConfluentCloudRestClientManager(Protocol):
    """Interface for managing Confluent Cloud REST client connections."""

    def get_confluent_cloud_flink_rest_client(self) -> httpx.AsyncClient:
        """Gets a configured REST client for Confluent Cloud Flink operations"""
        ...

    def get_confluent_cloud_rest_client(self) -> httpx.AsyncClient:
        """Gets a configured REST client for general Confluent Cloud operations"""
        ...

    def get_confluent_cloud_tableflow_rest_client(self) -> httpx.AsyncClient:
        """Gets a configured REST client for Tableflow operations"""
        ...

    def get_confluent_cloud_schema_registry_rest_client(self) -> httpx.AsyncClient:
        """Gets a configured REST client for Confluent Cloud Schema Registry operations"""
        ...

    def get_confluent_cloud_kafka_rest_client(self) -> httpx.AsyncClient:
        """Gets a configured REST client for Confluent Cloud Kafka operations"""
        ...

    def set_confluent_cloud_rest_endpoint(self, endpoint: str) -> None:
        ...

    def set_confluent_cloud_flink_endpoint(self, endpoint: str) -> None:
        ...

    def set_confluent_cloud_schema_registry_endpoint(self, endpoint: str) -> None:
        ...

    def set_confluent_cloud_kafka_rest_endpoint(self, endpoint: str) -> None:
        ...

    def set_confluent_cloud_tableflow_rest_endpoint(self, endpoint: str) -> None:
        ...


class SchemaRegistryClientHandler(Protocol):
    """Interface for managing Schema Registry client connections."""

    def get_schema_registry_client(self) -> SchemaRegistryClient:
        ...


class ClientManager(
    KafkaClientManager,
    ConfluentCloudRestClientManager,
    SchemaRegistryClientHandler,
    Protocol,
):
    """Combined interface for all client management"""
    pass


class ClientManagerConfig:
    """Configuration for all clients"""

    def __init__(
        self,
        kafka: Dict[str, Any],
        endpoints: ConfluentEndpoints,
        auth: Dict[str, ConfluentAuth],
    ):
        self.kafka = kafka
        self.endpoints = endpoints
        self.auth = auth


class DefaultClientManager(ClientManager):
    """
    Default implementation of client management for Kafka and Confluent Cloud services.
    Manages lifecycle and lazy initialization of various client connections.
    """

    def __init__(self, config: ClientManagerConfig):
        """
        Creates a new DefaultClientManager instance.
        
        Args:
            config: Configuration for all clients
        """
        self._confluent_cloud_base_url: Optional[str] = config.endpoints.cloud
        self._confluent_cloud_tableflow_base_url: Optional[str] = (
            config.endpoints.cloud  # At the time of writing, APIs are exposed on the same base URL
        )
        self._confluent_cloud_flink_base_url: Optional[str] = config.endpoints.flink
        self._confluent_cloud_schema_registry_base_url: Optional[str] = (
            config.endpoints.schema_registry
        )
        self._confluent_cloud_kafka_rest_base_url: Optional[str] = config.endpoints.kafka

        self._kafka_config: Dict[str, Any] = config.kafka

        # Lazy initialization of Kafka clients
        self._kafka_client: Lazy[Dict[str, Any]] = Lazy(
            lambda: self._create_kafka_client()
        )

        self._admin_client: AsyncLazy[AdminClient] = AsyncLazy(
            async_factory=self._create_admin_client,
            cleanup=self._cleanup_admin_client,
        )

        self._producer: AsyncLazy[Producer] = AsyncLazy(
            async_factory=self._create_producer,
            cleanup=self._cleanup_producer,
        )

        # Lazy initialization of REST clients
        self._confluent_cloud_rest_client: Lazy[httpx.AsyncClient] = Lazy(
            lambda: self._create_confluent_cloud_rest_client(config)
        )

        self._confluent_cloud_tableflow_rest_client: Lazy[httpx.AsyncClient] = Lazy(
            lambda: self._create_confluent_cloud_tableflow_rest_client(config)
        )

        self._confluent_cloud_flink_rest_client: Lazy[httpx.AsyncClient] = Lazy(
            lambda: self._create_confluent_cloud_flink_rest_client(config)
        )

        self._confluent_cloud_schema_registry_rest_client: Lazy[httpx.AsyncClient] = (
            Lazy(lambda: self._create_confluent_cloud_schema_registry_rest_client(config))
        )

        self._confluent_cloud_kafka_rest_client: Lazy[httpx.AsyncClient] = Lazy(
            lambda: self._create_confluent_cloud_kafka_rest_client(config)
        )

        # Lazy initialization of Schema Registry client
        self._schema_registry_client: Lazy[SchemaRegistryClient] = Lazy(
            lambda: self._create_schema_registry_client(config)
        )

    def _create_kafka_client(self) -> Dict[str, Any]:
        """Create Kafka client configuration"""
        logger.info("Initializing Kafka client configuration")
        return self._kafka_config

    async def _create_admin_client(self) -> AdminClient:
        """Create and connect admin client"""
        logger.info("Connecting Kafka Admin")
        admin = AdminClient(self._kafka_config)
        return admin

    async def _cleanup_admin_client(self, admin: AdminClient) -> None:
        """Cleanup admin client"""
        # AdminClient in confluent-kafka-python doesn't need explicit disconnect
        pass

    async def _create_producer(self) -> Producer:
        """Create and connect producer"""
        logger.info("Connecting Kafka Producer")
        producer = Producer(self._kafka_config)
        return producer

    async def _cleanup_producer(self, producer: Producer) -> None:
        """Cleanup producer"""
        producer.flush()

    def _create_confluent_cloud_rest_client(
        self, config: ClientManagerConfig
    ) -> httpx.AsyncClient:
        """Create Confluent Cloud REST client"""
        if not self._confluent_cloud_base_url:
            raise ValueError("Confluent Cloud REST endpoint not configured")

        logger.info(
            f"Initializing Confluent Cloud REST client for base URL {self._confluent_cloud_base_url}"
        )

        auth_middleware = create_auth_middleware(config.auth["cloud"])
        client = httpx.AsyncClient(
            base_url=self._confluent_cloud_base_url,
            auth=auth_middleware,
            timeout=30.0,
        )
        return client

    def _create_confluent_cloud_tableflow_rest_client(
        self, config: ClientManagerConfig
    ) -> httpx.AsyncClient:
        """Create Confluent Cloud Tableflow REST client"""
        if not self._confluent_cloud_tableflow_base_url:
            raise ValueError("Confluent Cloud Tableflow REST endpoint not configured")

        logger.info(
            f"Initializing Confluent Cloud Tableflow REST client for base URL {self._confluent_cloud_tableflow_base_url}"
        )

        auth_middleware = create_auth_middleware(config.auth["tableflow"])
        client = httpx.AsyncClient(
            base_url=self._confluent_cloud_tableflow_base_url,
            auth=auth_middleware,
            timeout=30.0,
        )
        return client

    def _create_confluent_cloud_flink_rest_client(
        self, config: ClientManagerConfig
    ) -> httpx.AsyncClient:
        """Create Confluent Cloud Flink REST client"""
        if not self._confluent_cloud_flink_base_url:
            raise ValueError("Confluent Cloud Flink REST endpoint not configured")

        logger.info(
            f"Initializing Confluent Cloud Flink REST client for base URL {self._confluent_cloud_flink_base_url}"
        )

        auth_middleware = create_auth_middleware(config.auth["flink"])
        client = httpx.AsyncClient(
            base_url=self._confluent_cloud_flink_base_url,
            auth=auth_middleware,
            timeout=30.0,
        )
        return client

    def _create_confluent_cloud_schema_registry_rest_client(
        self, config: ClientManagerConfig
    ) -> httpx.AsyncClient:
        """Create Confluent Cloud Schema Registry REST client"""
        if not self._confluent_cloud_schema_registry_base_url:
            raise ValueError(
                "Confluent Cloud Schema Registry REST endpoint not configured"
            )

        logger.info(
            f"Initializing Confluent Cloud Schema Registry REST client for base URL {self._confluent_cloud_schema_registry_base_url}"
        )

        auth_middleware = create_auth_middleware(config.auth["schemaRegistry"])
        client = httpx.AsyncClient(
            base_url=self._confluent_cloud_schema_registry_base_url,
            auth=auth_middleware,
            timeout=30.0,
        )
        return client

    def _create_confluent_cloud_kafka_rest_client(
        self, config: ClientManagerConfig
    ) -> httpx.AsyncClient:
        """Create Confluent Cloud Kafka REST client"""
        if not self._confluent_cloud_kafka_rest_base_url:
            raise ValueError("Confluent Cloud Kafka REST endpoint not configured")

        logger.info(
            f"Initializing Confluent Cloud Kafka REST client for base URL {self._confluent_cloud_kafka_rest_base_url}"
        )

        auth_middleware = create_auth_middleware(config.auth["kafka"])
        client = httpx.AsyncClient(
            base_url=self._confluent_cloud_kafka_rest_base_url,
            auth=auth_middleware,
            timeout=30.0,
        )
        return client

    def _create_schema_registry_client(
        self, config: ClientManagerConfig
    ) -> SchemaRegistryClient:
        """Create Schema Registry client"""
        if not self._confluent_cloud_schema_registry_base_url:
            raise ValueError("Schema Registry endpoint not configured")

        api_key = config.auth["schemaRegistry"].api_key
        api_secret = config.auth["schemaRegistry"].api_secret

        return SchemaRegistryClient({
            "url": self._confluent_cloud_schema_registry_base_url,
            "basic.auth.credentials.source": "USER_INFO",
            "basic.auth.user.info": f"{api_key}:{api_secret}",
        })

    async def get_consumer(self, session_id: Optional[str] = None) -> Consumer:
        """
        Get a consumer client with optional session-specific group ID.
        
        Args:
            session_id: Optional session identifier to append to group ID
            
        Returns:
            Connected consumer client
        """
        # Build the config inline, merging with defaults
        base_group_id = self._kafka_config.get("group.id", "mcp-confluent-cloud")
        group_id = f"{base_group_id}-{session_id}" if session_id else base_group_id

        consumer_config = {
            **self._kafka_config,
            "group.id": group_id,
            "auto.offset.reset": self._kafka_config.get("auto.offset.reset", "earliest"),
            "allow.auto.create.topics": self._kafka_config.get(
                "allow.auto.create.topics", False
            ),
            "enable.auto.commit": self._kafka_config.get("enable.auto.commit", False),
        }

        return Consumer(consumer_config)

    def set_confluent_cloud_rest_endpoint(self, endpoint: str) -> None:
        """
        Set a new Confluent Cloud REST endpoint. Closes the current client first.
        
        Args:
            endpoint: The endpoint URL to set
        """
        self._confluent_cloud_rest_client.close()
        self._confluent_cloud_base_url = endpoint

    def set_confluent_cloud_tableflow_rest_endpoint(self, endpoint: str) -> None:
        """Set a new Confluent Cloud Tableflow REST endpoint"""
        self._confluent_cloud_tableflow_rest_client.close()
        self._confluent_cloud_tableflow_base_url = endpoint

    def set_confluent_cloud_flink_endpoint(self, endpoint: str) -> None:
        """Set a new Confluent Cloud Flink endpoint"""
        self._confluent_cloud_flink_rest_client.close()
        self._confluent_cloud_flink_base_url = endpoint

    def set_confluent_cloud_schema_registry_endpoint(self, endpoint: str) -> None:
        """Set a new Confluent Cloud Schema Registry endpoint"""
        self._confluent_cloud_schema_registry_rest_client.close()
        self._confluent_cloud_schema_registry_base_url = endpoint

    def set_confluent_cloud_kafka_rest_endpoint(self, endpoint: str) -> None:
        """Set a new Confluent Cloud Kafka REST endpoint"""
        self._confluent_cloud_kafka_rest_client.close()
        self._confluent_cloud_kafka_rest_base_url = endpoint

    def get_kafka_client(self) -> Dict[str, Any]:
        """Get the Kafka client configuration"""
        return self._kafka_client.get()

    def get_confluent_cloud_flink_rest_client(self) -> httpx.AsyncClient:
        """Get Confluent Cloud Flink REST client"""
        return self._confluent_cloud_flink_rest_client.get()

    def get_confluent_cloud_rest_client(self) -> httpx.AsyncClient:
        """Get Confluent Cloud REST client"""
        return self._confluent_cloud_rest_client.get()

    def get_confluent_cloud_tableflow_rest_client(self) -> httpx.AsyncClient:
        """Get Confluent Cloud Tableflow REST client"""
        return self._confluent_cloud_tableflow_rest_client.get()

    def get_confluent_cloud_schema_registry_rest_client(self) -> httpx.AsyncClient:
        """Get Confluent Cloud Schema Registry REST client"""
        return self._confluent_cloud_schema_registry_rest_client.get()

    def get_confluent_cloud_kafka_rest_client(self) -> httpx.AsyncClient:
        """Get Confluent Cloud Kafka REST client"""
        return self._confluent_cloud_kafka_rest_client.get()

    async def get_admin_client(self) -> AdminClient:
        """Get admin client"""
        return await self._admin_client.get()

    async def get_producer(self) -> Producer:
        """Get producer client"""
        return await self._producer.get()

    async def disconnect(self) -> None:
        """Disconnect and cleanup all client connections"""
        await self._admin_client.close()
        await self._producer.close()
        self._kafka_client.close()
        
        # Close REST clients
        if self._confluent_cloud_rest_client.is_initialized():
            await self._confluent_cloud_rest_client.get().aclose()
        if self._confluent_cloud_flink_rest_client.is_initialized():
            await self._confluent_cloud_flink_rest_client.get().aclose()
        if self._confluent_cloud_tableflow_rest_client.is_initialized():
            await self._confluent_cloud_tableflow_rest_client.get().aclose()
        if self._confluent_cloud_schema_registry_rest_client.is_initialized():
            await self._confluent_cloud_schema_registry_rest_client.get().aclose()
        if self._confluent_cloud_kafka_rest_client.is_initialized():
            await self._confluent_cloud_kafka_rest_client.get().aclose()

    def get_schema_registry_client(self) -> SchemaRegistryClient:
        """Get Schema Registry client"""
        return self._schema_registry_client.get()