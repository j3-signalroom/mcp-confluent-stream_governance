"""
This module provides helper functions for working with Confluent Schema Registry.
It handles schema registration, serialization, and deserialization of messages
using various schema formats (AVRO, JSON, PROTOBUF).
"""

import json
from enum import Enum
from typing import Dict, Literal, Optional, TypedDict, Union
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.json_schema import (
    JSONDeserializer,
    JSONSerializer,
)
from confluent_kafka.schema_registry.protobuf import (
    ProtobufDeserializer,
    ProtobufSerializer,
)
from logger import logger


class SerdeType(str, Enum):
    """Enumeration for serialization/deserialization type (key or value)."""
    KEY = "KEY"
    VALUE = "VALUE"


# Supported schema types for Confluent Schema Registry.
# AVRO: Apache Avro binary format with schema evolution support
# JSON: JSON Schema format with validation
# PROTOBUF: Protocol Buffers format with backward compatibility
SchemaType = Literal["AVRO", "JSON", "PROTOBUF"]


class SchemaRegistryOptions(TypedDict, total=False):
    """
    Common options for schema registry operations.
    These options control how schemas are registered and used for serialization.
    """
    use_schema_registry: bool
    schema_type: SchemaType
    schema: str
    subject: str
    normalize: bool


class MessageOptions(TypedDict, total=False):
    """
    Options for message serialization/deserialization.
    Includes the message payload and schema registry configuration.
    """
    message: Union[bytes, dict, str]
    use_schema_registry: bool
    schema_type: SchemaType
    schema: str
    subject: str
    normalize: bool


class SchemaNeededResult(TypedDict):
    """Result when a schema is needed."""
    type: Literal["schema-needed"]
    latest_schema: str
    subject: str
    schema_type: str


class NoSchemaResult(TypedDict):
    """Result when no schema exists."""
    type: Literal["no-schema"]
    subject: str


# Result of checking if a schema is needed for a message.
# Can indicate:
# - A schema is needed and provides the latest schema details
# - No schema exists and needs to be provided
# - No schema action is needed
SchemaCheckResult = Union[SchemaNeededResult, NoSchemaResult, None]


def get_serializer(
    schema_type: Optional[SchemaType],
    registry: SchemaRegistryClient,
    serde_type: SerdeType,
    schema_str: Optional[str] = None,
    schema_id: Optional[int] = None,
) -> Union[AvroSerializer, JSONSerializer, ProtobufSerializer]:
    """
    Creates and returns the appropriate serializer instance based on schema type.
    The serializer is configured to use either a specific schema ID or the latest version.

    Args:
        schema_type: The type of schema (AVRO, JSON, PROTOBUF)
        registry: The schema registry client instance
        serde_type: Whether this is for key or value serialization
        schema_str: Optional schema string for the serializer
        schema_id: Optional schema ID to use for serialization

    Returns:
        The appropriate Serializer instance

    Raises:
        ValueError: If the schema type is unknown or unsupported
    """
    if not schema_type:
        raise ValueError("schemaType is required")

    # Configuration for serializer
    conf = {"auto.register.schemas": False}
    
    if schema_type == "AVRO":
        return AvroSerializer(
            registry,
            schema_str=schema_str,
            conf=conf,
        )
    elif schema_type == "JSON":
        return JSONSerializer(
            schema_str=schema_str,
            schema_registry_client=registry,
            conf=conf,
        )
    elif schema_type == "PROTOBUF":
        return ProtobufSerializer(
            msg_type=None,  # Will need to be provided based on your use case
            schema_registry_client=registry,
            conf=conf,
        )
    else:
        raise ValueError(f"Unknown schemaType: {schema_type}")


def get_deserializer(
    schema_type: Optional[SchemaType],
    registry: SchemaRegistryClient,
    serde_type: SerdeType,
) -> Union[AvroDeserializer, JSONDeserializer, ProtobufDeserializer]:
    """
    Creates and returns the appropriate deserializer instance based on schema type.
    The deserializer is configured to handle schema evolution and compatibility.

    Args:
        schema_type: The type of schema (AVRO, JSON, PROTOBUF)
        registry: The schema registry client instance
        serde_type: Whether this is for key or value deserialization

    Returns:
        The appropriate Deserializer instance

    Raises:
        ValueError: If the schema type is unknown or unsupported
    """
    if not schema_type:
        raise ValueError("schemaType is required")

    if schema_type == "AVRO":
        return AvroDeserializer(registry)
    elif schema_type == "JSON":
        return JSONDeserializer(
            schema_str=None,  # Will use schema from message
            schema_registry_client=registry,
        )
    elif schema_type == "PROTOBUF":
        return ProtobufDeserializer(
            message_type=None,  # Will be inferred from schema
            schema_registry_client=registry,
        )
    else:
        raise ValueError(f"Unknown schemaType: {schema_type}")


async def check_schema_needed(
    topic_name: str,
    options: MessageOptions,
    serde_type: SerdeType,
    registry: Optional[SchemaRegistryClient],
) -> SchemaCheckResult:
    """
    Checks if a schema is needed for the given message options.
    This function determines if:
    1. A schema is already registered and should be used
    2. No schema exists and needs to be provided
    3. No schema action is needed

    Args:
        topic_name: The Kafka topic name
        options: The message options including schema, type, and payload
        serde_type: Whether this is for key or value serialization
        registry: The schema registry client instance (if used)

    Returns:
        An object describing the schema state, or None if no schema action is needed
    """
    if options.get("use_schema_registry") and not options.get("schema"):
        subject = options.get("subject") or (
            f"{topic_name}-{'key' if serde_type == SerdeType.KEY else 'value'}"
        )
        
        latest = await get_latest_schema_if_exists(registry, subject) if registry else None
        
        if latest:
            return SchemaNeededResult(
                type="schema-needed",
                latest_schema=latest["schema"],
                subject=subject,
                schema_type=latest["schema_type"],
            )
        else:
            return NoSchemaResult(type="no-schema", subject=subject)
    
    return None


async def get_latest_schema_if_exists(
    registry: SchemaRegistryClient,
    subject: str,
) -> Optional[Dict[str, str]]:
    """
    Fetches the latest schema string and schema type for a given subject from the schema registry.
    Handles 404 errors gracefully by returning None when no schema exists.

    Args:
        registry: The schema registry client instance
        subject: The subject to look up in the registry

    Returns:
        A dict with the latest schema string and schema type, or None if not found

    Raises:
        Exception: If there's an unexpected error from the registry
    """
    try:
        latest = registry.get_latest_version(subject)
        # The docs say that when no schemaType is supplied, it's assumed to be AVRO
        schema_type = latest.schema.schema_type or "AVRO"
        return {
            "schema": latest.schema.schema_str,
            "schema_type": schema_type,
        }
    except Exception as err:
        # Check if it's a 404 error (subject not found)
        if hasattr(err, "http_status_code") and err.http_status_code == 404:
            return None
        raise


async def serialize_message(
    topic_name: str,
    options: MessageOptions,
    serde_type: SerdeType,
    registry: Optional[SchemaRegistryClient],
) -> Union[bytes, str]:
    """
    Serializes a message using the provided options and schema registry configuration.
    This function:
    1. Registers the schema if provided
    2. Validates the message type
    3. Creates the appropriate serializer
    4. Serializes the message

    Args:
        topic_name: The Kafka topic name
        options: The message options including schema, type, and payload
        serde_type: Whether this is for key or value serialization
        registry: The schema registry client instance (if used)

    Returns:
        The serialized message as bytes or string

    Raises:
        ValueError: If serialization fails, schema registration fails, or message type is invalid
    """
    if not options.get("use_schema_registry"):
        message = options["message"]
        if not isinstance(message, str):
            logger.warning(
                "Warning: Sending non-string message without schema registry. "
                "This may fail if the topic expects a schema."
            )
        return message if isinstance(message, str) else json.dumps(message)

    if not options.get("schema_type"):
        raise ValueError("schema_type is required when use_schema_registry is True")

    if not registry:
        raise ValueError("Schema Registry client is required for serialization")

    # Default subject naming
    subject = options.get("subject") or (
        f"{topic_name}-{'key' if serde_type == SerdeType.KEY else 'value'}"
    )

    schema_id: Optional[int] = None
    schema_str = options.get("schema")

    # Register schema if provided
    if schema_str:
        try:
            from confluent_kafka.schema_registry import Schema
            
            schema = Schema(
                schema_str=schema_str,
                schema_type=options["schema_type"],
            )
            schema_id = registry.register_schema(
                subject_name=subject,
                schema=schema,
                normalize_schemas=options.get("normalize", False),
            )
        except Exception as err:
            raise ValueError(
                f"Failed to register schema for subject '{subject}': {err}"
            )

    # Validate message type
    message = options["message"]
    if not isinstance(message, dict):
        raise ValueError(
            "When using schema registry, message must be a dict matching the schema."
        )

    try:
        serializer = get_serializer(
            options["schema_type"],
            registry,
            serde_type,
            schema_str=schema_str,
            schema_id=schema_id,
        )
    except Exception as err:
        raise ValueError(f"Failed to get serializer: {err}")

    try:
        # The Python serializers expect (object, SerializationContext) but we can pass None for context
        return serializer(message, None)
    except Exception as err:
        raise ValueError(
            f"Failed to serialize message for subject '{subject}': {err}"
        )


async def deserialize_message(
    topic: str,
    message: bytes,
    schema_type: SchemaType,
    registry: SchemaRegistryClient,
    serde_type: SerdeType,
) -> any:
    """
    Deserializes a message using Schema Registry.
    This function:
    1. Creates the appropriate deserializer
    2. Deserializes the message using the schema from the registry

    Args:
        topic: The Kafka topic name
        message: The message bytes to deserialize
        schema_type: The schema type (AVRO, JSON, PROTOBUF)
        registry: The schema registry client
        serde_type: Whether this is key or value

    Returns:
        The deserialized object

    Raises:
        ValueError: If deserialization fails
    """
    try:
        deserializer = get_deserializer(schema_type, registry, serde_type)
        # The Python deserializers expect (bytes, SerializationContext) but we can pass None for context
        return deserializer(message, None)
    except Exception as err:
        raise ValueError(f"Failed to deserialize message: {err}")