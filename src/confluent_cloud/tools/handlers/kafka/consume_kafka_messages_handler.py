from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
import asyncio
import json
from mcp.types import CallToolResult

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from pydantic import BaseModel, Field, field_validator

from client_manager import ClientManager
from schema_registry_helper import (
    deserialize_message,
    get_latest_schema_if_exists,
)
from tools.base_tools import BaseToolHandler, ToolConfig
from tools.tool_name import ToolName
from logger import logger


class MessageOptions(BaseModel):
    """Options for message deserialization."""
    
    use_schema_registry: bool = Field(
        default=False,
        description="Whether to use schema registry for deserialization. If false, messages will be returned as raw.",
    )
    subject: Optional[str] = Field(
        default=None,
        description="Schema registry subject. Defaults to 'topicName-value' or 'topicName-key'.",
    )


class ValueOptions(MessageOptions):
    """Options for value deserialization."""
    pass


class KeyOptions(MessageOptions):
    """Options for key deserialization."""
    pass


class ConsumeKafkaMessagesArgs(BaseModel):
    """Arguments for consuming Kafka messages."""
    
    topic_names: List[str] = Field(
        ...,
        min_length=1,
        description="Names of the Kafka topics to consume from.",
    )
    max_messages: int = Field(
        default=10,
        gt=0,
        description="Maximum number of messages to consume before stopping.",
    )
    timeout_ms: int = Field(
        default=10000,
        gt=0,
        description="Maximum time in milliseconds to wait for messages before stopping.",
    )
    value: ValueOptions
    key: Optional[KeyOptions] = None

    @field_validator("topic_names")
    @classmethod
    def validate_topic_names(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("topic_names cannot be empty")
        return v


@dataclass
class ProcessedMessage:
    """A processed Kafka message with deserialized key and value."""
    
    key: Any
    value: Any
    timestamp: str
    offset: str
    headers: Optional[Dict[str, str]]
    topic: str
    partition: int


class ConsumeKafkaMessagesHandler(BaseToolHandler):
    """
    Handler for consuming messages from Kafka topics with support for Schema Registry deserialization.
    This handler allows consuming messages from one or more topics with configurable message limits and timeouts.
    It supports automatic deserialization of Schema Registry encoded messages (AVRO, JSON, PROTOBUF).
    """

    async def process_message(
        self,
        topic: str,
        partition: int,
        message: Any,
        registry: Optional[SchemaRegistryClient],
        value_options: ValueOptions,
        key_options: Optional[KeyOptions] = None,
    ) -> ProcessedMessage:
        """
        Processes a single Kafka message, handling deserialization of both key and value.
        
        Args:
            topic: The topic the message was consumed from
            partition: The partition the message was consumed from
            message: The raw Kafka message
            registry: Optional Schema Registry client for deserialization
            value_options: Options for value deserialization
            key_options: Optional options for key deserialization
            
        Returns:
            A processed message with deserialized key and value
        """
        processed_key: Any = message.key().decode("utf-8") if message.key() else None
        processed_value: Any = message.value().decode("utf-8") if message.value() else None

        async def deserialize_with_options(
            buffer: Optional[bytes],
            options: Union[ValueOptions, KeyOptions],
            serde_type: str,
        ) -> Any:
            """Helper function to deserialize with given options."""
            if not options.use_schema_registry or not registry:
                return buffer.decode("utf-8") if buffer else None
            
            subject = options.subject or f"{topic}-{serde_type}"
            schema = await get_latest_schema_if_exists(registry, subject)
            
            if not schema or not schema.get("schemaType"):
                return buffer.decode("utf-8") if buffer else None
            
            try:
                return await deserialize_message(
                    topic,
                    buffer,
                    schema["schemaType"],
                    registry,
                    serde_type,
                )
            except Exception as err:
                logger.error(
                    {
                        "error": str(err),
                        "topic": topic,
                        "schemaType": schema.get("schemaType"),
                        "serdeType": serde_type,
                    },
                    f"Error deserializing message {serde_type} for topic {topic}",
                )
                return buffer.decode("utf-8") if buffer else None

        # Deserialize value
        processed_value = await deserialize_with_options(
            message.value(),
            value_options,
            "value",
        )

        # Deserialize key if options provided
        if message.key() and key_options:
            processed_key = await deserialize_with_options(
                message.key(),
                key_options,
                "key",
            )

        # Process headers
        headers = None
        if message.headers():
            headers = {
                key: value.decode("utf-8") if isinstance(value, bytes) else str(value)
                for key, value in message.headers()
            }

        return ProcessedMessage(
            key=processed_key,
            value=processed_value,
            timestamp=str(message.timestamp()[1]) if message.timestamp()[0] >= 0 else "",
            offset=str(message.offset()),
            headers=headers,
            topic=topic,
            partition=partition,
        )

    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: Dict[str, Any],
        session_id: Optional[str] = None,
    ) -> CallToolResult:
        """
        Main handler for consuming messages from Kafka topics.
        
        Args:
            client_manager: The client manager for Kafka and registry clients
            tool_arguments: The arguments for the tool, including topics, message limits, and deserialization options
            session_id: Optional session ID for Kafka consumer
            
        Returns:
            A CallToolResult containing the consumed messages or error information
        """
        # Parse and validate arguments
        args = ConsumeKafkaMessagesArgs(**tool_arguments)
        
        consumed_messages: List[ProcessedMessage] = []
        timeout_reached = False
        consumer: Optional[Consumer] = None
        
        # Get Schema Registry client if needed
        registry: Optional[SchemaRegistryClient] = None
        if args.value.use_schema_registry or (args.key and args.key.use_schema_registry):
            registry = client_manager.get_schema_registry_client()

        try:
            # Get consumer from client manager
            consumer = await client_manager.get_consumer(session_id)
            
            # Subscribe to topics
            consumer.subscribe(args.topic_names)
            
            # Calculate timeout in seconds
            timeout_seconds = args.timeout_ms / 1000.0
            start_time = asyncio.get_event_loop().time()
            
            # Consume messages
            while not timeout_reached and len(consumed_messages) < args.max_messages:
                # Check timeout
                elapsed = asyncio.get_event_loop().time() - start_time
                if elapsed >= timeout_seconds:
                    timeout_reached = True
                    break
                
                # Poll for messages (non-blocking with short timeout)
                remaining_time = timeout_seconds - elapsed
                poll_timeout = min(1.0, remaining_time)
                
                msg = consumer.poll(timeout=poll_timeout)
                
                if msg is None:
                    # No message available, continue polling
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, continue
                        continue
                    else:
                        raise KafkaException(msg.error())
                
                # Process the message
                processed = await self.process_message(
                    msg.topic(),
                    msg.partition(),
                    msg,
                    registry,
                    args.value,
                    args.key,
                )
                consumed_messages.append(processed)
                
                # Check if we've reached max messages
                if len(consumed_messages) >= args.max_messages:
                    break
            
            # Format response
            messages_dict = [
                {
                    "key": msg.key,
                    "value": msg.value,
                    "timestamp": msg.timestamp,
                    "offset": msg.offset,
                    "headers": msg.headers,
                    "topic": msg.topic,
                    "partition": msg.partition,
                }
                for msg in consumed_messages
            ]
            
            return self.create_response(
                f"Consumed {len(consumed_messages)} messages from topics {', '.join(args.topic_names)}.\n"
                f"Consumed messages: {json.dumps(messages_dict, indent=2)}",
                is_error=False,
            )
            
        except KafkaException as error:
            error_message = f"Kafka error ({error.args[0].code()}): {error.args[0].str()}"
            return self.create_response(
                f"Failed to consume messages: {error_message}",
                is_error=True,
            )
        except Exception as error:
            error_message = str(error)
            return self.create_response(
                f"Failed to consume messages: {error_message}",
                is_error=True,
            )
        finally:
            if consumer:
                try:
                    consumer.close()
                except Exception as error:
                    logger.error({"error": str(error)}, "Error cleaning up consumer")

    def get_tool_config(self) -> ToolConfig:
        """Returns the tool configuration."""
        return ToolConfig(
            name=ToolName.CONSUME_MESSAGES,
            description=(
                "Consumes messages from one or more Kafka topics. "
                "Supports automatic deserialization of Schema Registry encoded messages (AVRO, JSON, PROTOBUF)."
            ),
            input_schema=ConsumeKafkaMessagesArgs.model_json_schema(),
        )

    def get_required_env_vars(self) -> List[str]:
        """Returns the required environment variables."""
        return ["KAFKA_API_KEY", "KAFKA_API_SECRET", "BOOTSTRAP_SERVERS"]