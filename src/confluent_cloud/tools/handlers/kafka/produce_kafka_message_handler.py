from typing import Any, Literal
from pydantic import BaseModel, Field
from client_manager import ClientManager
from schema_registry_helper import (
    check_schema_needed,
    serialize_message,
    MessageOptions as SRMessageOptions,
    SchemaCheckResult,
    SerdeType
)
from mcp.types import CallToolResult
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName


class MessageOptions(BaseModel):
    """Options for message serialization"""
    message: dict[str, Any] | str = Field(
        ...,
        description=(
            "The payload to produce. If using schema registry, this should be an object "
            "matching the schema. Otherwise, a string."
        )
    )
    use_schema_registry: bool = Field(
        default=False,
        description="If true, use schema registry for serialization. If false, send as raw string/JSON."
    )
    schema_type: Literal["AVRO", "JSON", "PROTOBUF"] | None = Field(
        default=None,
        description="Schema type to use. If omitted, sends as raw string/JSON."
    )
    schema: str | None = Field(
        default=None,
        description=(
            "Schema definition to register (as JSON string for AVRO/JSON, or .proto for PROTOBUF). "
            "If omitted, uses latest registered schema."
        )
    )
    subject: str | None = Field(
        default=None,
        description="Schema Registry subject. Defaults to {topicName}-value or {topicName}-key."
    )
    normalize: bool | None = None


class ProduceKafkaMessageArguments(BaseModel):
    """Arguments for producing a Kafka message"""
    topic_name: str = Field(
        ...,
        min_length=1,
        description="Name of the kafka topic to produce the message to"
    )
    value: MessageOptions
    key: MessageOptions | None = None


class ProduceKafkaMessageHandler(BaseToolHandler):
    """
    Handler for producing messages to a Kafka topic, with support for Confluent Schema Registry 
    serialization (AVRO, JSON, PROTOBUF) for both key and value.
    
    - If schema registry is enabled, the handler checks if a schema is already registered for the topic's key/value subject.
      - If a schema exists and none is provided, it returns the latest schema to the client for retry.
      - If no schema exists and none is provided, it returns an error.
      - If a schema is provided, it is registered before serialization.
    - Serialization is performed using the appropriate serializer for the schema type.
    - Produces the message to the specified Kafka topic, handling both key and value serialization as needed.
    """
    
    def handle_schema_check_result(self, result: SchemaCheckResult | None) -> CallToolResult | None:
        """
        Handles the result of a schema check, returning a CallToolResult if a schema issue is found, or None otherwise.
        
        Args:
            result: The schema check result to handle
            
        Returns:
            A CallToolResult if a schema issue is found, or None otherwise
        """
        if not result:
            return None
        
        if result.type == "schema-needed":
            return self.create_response(
                f"A schema already exists for subject '{result.subject}'. Please retry with the following schema:\n"
                f"{result.latest_schema}, schemaType: {result.schema_type}",
                is_error=True,
                meta={
                    "latest_schema": result.latest_schema,
                    "subject": result.subject,
                    "schema_type": result.schema_type,
                }
            )
        else:
            return self.create_response(
                f"No schema registered for subject '{result.subject}', and no schema provided to register.",
                is_error=True
            )
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Main handler for producing a message to a Kafka topic, including schema registry logic and serialization.
        Handles both value and key, and returns a CallToolResult with the outcome.
        
        Args:
            client_manager: The client manager for Kafka and registry clients
            tool_arguments: The arguments for the tool, including topic, value, and key
            session_id: Optional session identifier
            
        Returns:
            A CallToolResult describing the outcome of the produce operation
        """
        if tool_arguments is None:
            return self.create_response(
                "No arguments provided for producing message",
                is_error=True
            )
        
        try:
            # Parse and validate arguments
            args = ProduceKafkaMessageArguments.model_validate(tool_arguments)
        except Exception as e:
            return self.create_response(
                f"Invalid arguments: {str(e)}",
                is_error=True
            )
        
        # Only create registry if needed
        needs_registry = args.value.use_schema_registry or (args.key and args.key.use_schema_registry)
        registry = client_manager.get_schema_registry_client() if needs_registry else None
        
        # Convert to SR message options format
        value_sr_options = SRMessageOptions(**args.value.model_dump())
        key_sr_options = SRMessageOptions(**args.key.model_dump()) if args.key else None
        
        # Check for latest schema if needed (value)
        value_schema_check = await check_schema_needed(
            args.topic_name,
            value_sr_options,
            SerdeType.VALUE,
            registry,
        )
        value_schema_result = self.handle_schema_check_result(value_schema_check)
        if value_schema_result:
            return value_schema_result
        
        # Check for latest schema if needed (key)
        if args.key:
            key_schema_check = await check_schema_needed(
                args.topic_name,
                key_sr_options,
                SerdeType.KEY,
                registry,
            )
            key_schema_result = self.handle_schema_check_result(key_schema_check)
            if key_schema_result:
                return key_schema_result
        
        # Serialize messages
        try:
            value_to_send = await serialize_message(
                args.topic_name,
                value_sr_options,
                SerdeType.VALUE,
                registry,
            )
            
            key_to_send = None
            if args.key:
                key_to_send = await serialize_message(
                    args.topic_name,
                    key_sr_options,
                    SerdeType.KEY,
                    registry,
                )
        except Exception as err:
            return self.create_response(
                f"Failed to serialize: {str(err)}",
                is_error=True
            )
        
        # Send the message
        try:
            producer = await client_manager.get_producer()
            
            # Build message
            message_dict = {"value": value_to_send}
            if key_to_send is not None:
                message_dict["key"] = key_to_send
            
            # Send to Kafka
            delivery_report = await producer.send(
                args.topic_name,
                **message_dict
            )
            
            # Format response (assuming delivery_report is RecordMetadata or similar)
            if hasattr(delivery_report, 'error_code'):
                error_code = delivery_report.error_code
            else:
                error_code = 0
            
            if error_code != 0:
                formatted_response = (
                    f"Error producing message to [Topic: {delivery_report.topic}, "
                    f"Partition: {delivery_report.partition}, Offset: {delivery_report.offset} "
                    f"with ErrorCode: {error_code}]"
                )
                is_error = True
            else:
                formatted_response = (
                    f"Message produced successfully to [Topic: {delivery_report.topic}, "
                    f"Partition: {delivery_report.partition}, Offset: {delivery_report.offset}]"
                )
                is_error = False
            
            return self.create_response(formatted_response, is_error=is_error)
            
        except Exception as err:
            return self.create_response(
                f"Failed to produce message: {str(err)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Returns the tool configuration including name, description, and input schema"""
        return ToolConfig(
            name=ToolName.PRODUCE_MESSAGE,
            description=(
                f"Produce records to a Kafka topic. Supports Confluent Schema Registry serialization "
                f"(AVRO, JSON, PROTOBUF) for both key and value.\n\n"
                f"Before producing, check if the topic has a registered schema for <topicName>-value and "
                f"<topicName>-key. If a schema exists, set useSchemaRegistry to true and specify the "
                f"appropriate schemaType. For saving user messages/history, use the kafka topic named "
                f"mcp-conversations unless otherwise specified. If the topic does not exist, it can be "
                f"created via the {ToolName.CREATE_TOPICS.value} tool."
            ),
            inputSchema=ProduceKafkaMessageArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["KAFKA_API_KEY", "KAFKA_API_SECRET", "BOOTSTRAP_SERVERS"]