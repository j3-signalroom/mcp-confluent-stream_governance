"""
Environment variable schema definitions with validation.
Uses Pydantic for runtime validation and type safety.
"""

from typing import Optional, Literal
from pydantic import BaseModel, Field, HttpUrl, field_validator, ConfigDict

from logger import log_levels


# Dynamically generate LogLevel type from log_levels keys
LogLevel = Literal[tuple(log_levels.keys())]  # type: ignore


class EnvSchema(BaseModel):
    """
    Environment variables that are required for tools to be enabled/disabled.
    """
    
    HTTP_PORT: int = Field(
        default=8080,
        gt=0,
        description="Port to use for HTTP transport"
    )
    
    HTTP_HOST: str = Field(
        default="0.0.0.0",
        description="Host to bind for HTTP transport. 0.0.0.0 means all interfaces."
    )
    
    HTTP_MCP_ENDPOINT_PATH: str = Field(
        default="/mcp",
        description="HTTP endpoint path for MCP transport (e.g., '/mcp')"
    )
    
    SSE_MCP_ENDPOINT_PATH: str = Field(
        default="/sse",
        description="SSE endpoint path for establishing SSE connections (e.g., '/sse', '/events')"
    )
    
    SSE_MCP_MESSAGE_ENDPOINT_PATH: str = Field(
        default="/messages",
        description="SSE message endpoint path for receiving messages (e.g., '/messages', '/events/messages')"
    )
    
    LOG_LEVEL: LogLevel = Field(
        default="info",
        description=f"Log level for application logging ({', '.join(log_levels.keys())})"
    )
    
    BOOTSTRAP_SERVERS: Optional[str] = Field(
        default=None,
        description="List of Kafka broker addresses in the format host1:port1,host2:port2 used to establish initial connection to the Kafka cluster"
    )
    
    KAFKA_API_KEY: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Authentication credential (username) required to establish secure connection with the Kafka cluster"
    )
    
    KAFKA_API_SECRET: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Authentication credential (password) paired with KAFKA_API_KEY for secure Kafka cluster access"
    )
    
    FLINK_API_KEY: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Authentication key for accessing Confluent Cloud's Flink services, including compute pools and SQL statement management"
    )
    
    FLINK_API_SECRET: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Secret token paired with FLINK_API_KEY for authenticated access to Confluent Cloud's Flink services"
    )
    
    CONFLUENT_CLOUD_API_KEY: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Master API key for Confluent Cloud platform administration, enabling management of resources across your organization"
    )
    
    CONFLUENT_CLOUD_API_SECRET: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Master API secret paired with CONFLUENT_CLOUD_API_KEY for comprehensive Confluent Cloud platform administration"
    )
    
    SCHEMA_REGISTRY_API_KEY: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Authentication key for accessing Schema Registry services to manage and validate data schemas"
    )
    
    SCHEMA_REGISTRY_API_SECRET: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Authentication secret paired with SCHEMA_REGISTRY_API_KEY for secure Schema Registry access"
    )
    
    TABLEFLOW_API_KEY: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Authentication key for accessing Confluent Cloud's Tableflow services"
    )
    
    TABLEFLOW_API_SECRET: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Authentication secret paired with TABLEFLOW_API_KEY for secure Tableflow access"
    )
    
    @field_validator('LOG_LEVEL', mode='before')
    @classmethod
    def normalize_log_level(cls, v: any) -> str:
        """Normalize log level to lowercase and validate against log_levels"""
        if isinstance(v, str):
            normalized = v.lower()
            if normalized not in log_levels:
                valid_levels = ', '.join(log_levels.keys())
                raise ValueError(
                    f"Invalid log level: {v}. Must be one of: {valid_levels}"
                )
            return normalized
        return v
    
    model_config = ConfigDict(
        extra='allow',
        str_strip_whitespace=True,
    )


class ConfigSchema(BaseModel):
    """
    Environment variables that are optional for tools / could be provided at runtime.
    All fields are optional.
    """
    
    FLINK_ENV_ID: Optional[str] = Field(
        default=None,
        description="Unique identifier for the Flink environment, must start with 'env-' prefix"
    )
    
    FLINK_ORG_ID: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Organization identifier within Confluent Cloud for Flink resource management"
    )
    
    FLINK_REST_ENDPOINT: Optional[HttpUrl] = Field(
        default=None,
        description="Base URL for Confluent Cloud's Flink REST API endpoints used for SQL statement and compute pool management"
    )
    
    FLINK_COMPUTE_POOL_ID: Optional[str] = Field(
        default=None,
        description="Unique identifier for the Flink compute pool, must start with 'lfcp-' prefix"
    )
    
    FLINK_ENV_NAME: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Human-readable name for the Flink environment used for identification and display purposes"
    )
    
    FLINK_DATABASE_NAME: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Name of the associated Kafka cluster used as a database reference in Flink SQL operations"
    )
    
    KAFKA_CLUSTER_ID: Optional[str] = Field(
        default=None,
        min_length=1,
        description="Unique identifier for the Kafka cluster within Confluent Cloud ecosystem"
    )
    
    KAFKA_ENV_ID: Optional[str] = Field(
        default=None,
        description="Environment identifier for Kafka cluster, must start with 'env-' prefix"
    )
    
    CONFLUENT_CLOUD_REST_ENDPOINT: HttpUrl = Field(
        default="https://api.confluent.cloud",
        description="Base URL for Confluent Cloud's REST API services"
    )
    
    SCHEMA_REGISTRY_ENDPOINT: Optional[HttpUrl] = Field(
        default=None,
        description="URL endpoint for accessing Schema Registry services to manage data schemas"
    )
    
    KAFKA_REST_ENDPOINT: Optional[HttpUrl] = Field(
        default=None,
        description="REST API endpoint for Kafka cluster management and administration"
    )
    
    @field_validator('FLINK_ENV_ID', mode='after')
    @classmethod
    def validate_flink_env_id(cls, v: Optional[str]) -> Optional[str]:
        """Validate FLINK_ENV_ID starts with 'env-' prefix"""
        if v is not None and not v.startswith('env-'):
            raise ValueError("FLINK_ENV_ID must start with 'env-' prefix")
        return v
    
    @field_validator('FLINK_COMPUTE_POOL_ID', mode='after')
    @classmethod
    def validate_flink_compute_pool_id(cls, v: Optional[str]) -> Optional[str]:
        """Validate FLINK_COMPUTE_POOL_ID starts with 'lfcp-' prefix"""
        if v is not None and not v.startswith('lfcp-'):
            raise ValueError("FLINK_COMPUTE_POOL_ID must start with 'lfcp-' prefix")
        return v
    
    @field_validator('KAFKA_ENV_ID', mode='after')
    @classmethod
    def validate_kafka_env_id(cls, v: Optional[str]) -> Optional[str]:
        """Validate KAFKA_ENV_ID starts with 'env-' prefix"""
        if v is not None and not v.startswith('env-'):
            raise ValueError("KAFKA_ENV_ID must start with 'env-' prefix")
        return v
    
    model_config = ConfigDict(
        extra='allow',
        str_strip_whitespace=True,
    )


class CombinedSchema(EnvSchema, ConfigSchema):
    """
    Combined schema merging both EnvSchema and ConfigSchema.
    Provides complete environment configuration with validation.
    """
    
    model_config = ConfigDict(
        extra='allow',
        str_strip_whitespace=True,
    )


# Dynamically generate EnvVar type from CombinedSchema field names
EnvVar = Literal[tuple(CombinedSchema.model_fields.keys())]  # type: ignore