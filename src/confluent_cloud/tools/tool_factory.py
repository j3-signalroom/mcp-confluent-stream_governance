from typing import ClassVar
from enum import Enum

from .base_tools import ToolHandler, ToolConfig
from .handlers.catalog.add_tags_to_topics_handler import AddTagToTopicHandler
from .handlers.catalog.create_topic_tags_handler import CreateTopicTagsHandler
from .handlers.catalog.delete_tag_handler import DeleteTagHandler
from .handlers.catalog.list_tags_handler import ListTagsHandler
from .handlers.catalog.remove_tag_from_entity_handler import RemoveTagFromEntityHandler
from .handlers.clusters.list_clusters_handler import ListClustersHandler
from .handlers.connect.create_connector_handler import CreateConnectorHandler
from .handlers.connect.delete_connector_handler import DeleteConnectorHandler
from .handlers.connect.list_connectors_handler import ListConnectorsHandler
from .handlers.connect.read_connectors_handler import ReadConnectorHandler
from .handlers.environments.list_environments_handler import ListEnvironmentsHandler
from .handlers.environments.read_environment_handler import ReadEnvironmentHandler
from .handlers.flink.create_flink_statement_handler import CreateFlinkStatementHandler
from .handlers.flink.delete_flink_statement_handler import DeleteFlinkStatementHandler
from .handlers.flink.list_flink_statements_handler import ListFlinkStatementsHandler
from .handlers.flink.read_flink_statement_handler import ReadFlinkStatementHandler
from .handlers.kafka.alter_topic_config import AlterTopicConfigHandler
from .handlers.kafka.consume_kafka_messages_handler import ConsumeKafkaMessagesHandler
from .handlers.kafka.create_topics_handler import CreateTopicsHandler
from .handlers.kafka.delete_topics_handler import DeleteTopicsHandler
from .handlers.kafka.get_topic_config import GetTopicConfigHandler
from .handlers.kafka.list_topics_handler import ListTopicsHandler
from .handlers.kafka.produce_kafka_message_handler import ProduceKafkaMessageHandler
from .handlers.schema.list_schemas_handler import ListSchemasHandler
from .handlers.search.search_topic_by_tag_handler import SearchTopicsByTagHandler
from .handlers.search.search_topics_by_name_handler import SearchTopicsByNameHandler
from .handlers.tableflow.catalog.create_tableflow_catalog_integration_handler import (
    CreateTableFlowCatalogIntegrationHandler,
)
from .handlers.tableflow.catalog.delete_tableflow_catalog_integration_handler import (
    DeleteTableFlowCatalogIntegrationHandler,
)
from .handlers.tableflow.catalog.list_tableflow_catalog_integrations_handler import (
    ListTableFlowCatalogIntegrationsHandler,
)
from .handlers.tableflow.catalog.read_tableflow_catalog_integration_handler import (
    ReadTableFlowCatalogIntegrationHandler,
)
from .handlers.tableflow.catalog.update_tableflow_catalog_integration_handler import (
    UpdateTableFlowCatalogIntegrationHandler,
)
from .handlers.tableflow.list_tableflow_regions_handler import ListTableFlowRegionsHandler
from .handlers.tableflow.topic.create_tableflow_topic_handler import CreateTableFlowTopicHandler
from .handlers.tableflow.topic.delete_tableflow_topic_handler import DeleteTableFlowTopicHandler
from .handlers.tableflow.topic.list_tableflow_topics_handler import ListTableFlowTopicsHandler
from .handlers.tableflow.topic.read_tableflow_topic_handler import ReadTableFlowTopicHandler
from .handlers.tableflow.topic.update_tableflow_topic_handler import UpdateTableFlowTopicHandler


class ToolName(str, Enum):
    """Enum of available tool names"""
    LIST_TOPICS = "list_topics"
    CREATE_TOPICS = "create_topics"
    DELETE_TOPICS = "delete_topics"
    PRODUCE_MESSAGE = "produce_message"
    LIST_FLINK_STATEMENTS = "list_flink_statements"
    CREATE_FLINK_STATEMENT = "create_flink_statement"
    READ_FLINK_STATEMENT = "read_flink_statement"
    DELETE_FLINK_STATEMENTS = "delete_flink_statements"
    LIST_CONNECTORS = "list_connectors"
    READ_CONNECTOR = "read_connector"
    CREATE_CONNECTOR = "create_connector"
    SEARCH_TOPICS_BY_TAG = "search_topics_by_tag"
    CREATE_TOPIC_TAGS = "create_topic_tags"
    DELETE_TAG = "delete_tag"
    REMOVE_TAG_FROM_ENTITY = "remove_tag_from_entity"
    ADD_TAGS_TO_TOPIC = "add_tags_to_topic"
    LIST_TAGS = "list_tags"
    ALTER_TOPIC_CONFIG = "alter_topic_config"
    DELETE_CONNECTOR = "delete_connector"
    SEARCH_TOPICS_BY_NAME = "search_topics_by_name"
    LIST_CLUSTERS = "list_clusters"
    LIST_ENVIRONMENTS = "list_environments"
    READ_ENVIRONMENT = "read_environment"
    LIST_SCHEMAS = "list_schemas"
    CONSUME_MESSAGES = "consume_messages"
    GET_TOPIC_CONFIG = "get_topic_config"
    CREATE_TABLEFLOW_TOPIC = "create_tableflow_topic"
    LIST_TABLEFLOW_REGIONS = "list_tableflow_regions"
    LIST_TABLEFLOW_TOPICS = "list_tableflow_topics"
    READ_TABLEFLOW_TOPIC = "read_tableflow_topic"
    UPDATE_TABLEFLOW_TOPIC = "update_tableflow_topic"
    DELETE_TABLEFLOW_TOPIC = "delete_tableflow_topic"
    CREATE_TABLEFLOW_CATALOG_INTEGRATION = "create_tableflow_catalog_integration"
    READ_TABLEFLOW_CATALOG_INTEGRATION = "read_tableflow_catalog_integration"
    LIST_TABLEFLOW_CATALOG_INTEGRATIONS = "list_tableflow_catalog_integrations"
    UPDATE_TABLEFLOW_CATALOG_INTEGRATION = "update_tableflow_catalog_integration"
    DELETE_TABLEFLOW_CATALOG_INTEGRATION = "delete_tableflow_catalog_integration"


class ToolFactory:
    """Factory for creating and managing tool handlers"""
    
    _handlers: ClassVar[dict[ToolName, ToolHandler]] = {
        ToolName.LIST_TOPICS: ListTopicsHandler(),
        ToolName.CREATE_TOPICS: CreateTopicsHandler(),
        ToolName.DELETE_TOPICS: DeleteTopicsHandler(),
        ToolName.PRODUCE_MESSAGE: ProduceKafkaMessageHandler(),
        ToolName.LIST_FLINK_STATEMENTS: ListFlinkStatementsHandler(),
        ToolName.CREATE_FLINK_STATEMENT: CreateFlinkStatementHandler(),
        ToolName.READ_FLINK_STATEMENT: ReadFlinkStatementHandler(),
        ToolName.DELETE_FLINK_STATEMENTS: DeleteFlinkStatementHandler(),
        ToolName.LIST_CONNECTORS: ListConnectorsHandler(),
        ToolName.READ_CONNECTOR: ReadConnectorHandler(),
        ToolName.CREATE_CONNECTOR: CreateConnectorHandler(),
        ToolName.SEARCH_TOPICS_BY_TAG: SearchTopicsByTagHandler(),
        ToolName.CREATE_TOPIC_TAGS: CreateTopicTagsHandler(),
        ToolName.DELETE_TAG: DeleteTagHandler(),
        ToolName.REMOVE_TAG_FROM_ENTITY: RemoveTagFromEntityHandler(),
        ToolName.ADD_TAGS_TO_TOPIC: AddTagToTopicHandler(),
        ToolName.LIST_TAGS: ListTagsHandler(),
        ToolName.ALTER_TOPIC_CONFIG: AlterTopicConfigHandler(),
        ToolName.DELETE_CONNECTOR: DeleteConnectorHandler(),
        ToolName.SEARCH_TOPICS_BY_NAME: SearchTopicsByNameHandler(),
        ToolName.LIST_CLUSTERS: ListClustersHandler(),
        ToolName.LIST_ENVIRONMENTS: ListEnvironmentsHandler(),
        ToolName.READ_ENVIRONMENT: ReadEnvironmentHandler(),
        ToolName.LIST_SCHEMAS: ListSchemasHandler(),
        ToolName.CONSUME_MESSAGES: ConsumeKafkaMessagesHandler(),
        ToolName.GET_TOPIC_CONFIG: GetTopicConfigHandler(),
        ToolName.CREATE_TABLEFLOW_TOPIC: CreateTableFlowTopicHandler(),
        ToolName.LIST_TABLEFLOW_REGIONS: ListTableFlowRegionsHandler(),
        ToolName.LIST_TABLEFLOW_TOPICS: ListTableFlowTopicsHandler(),
        ToolName.READ_TABLEFLOW_TOPIC: ReadTableFlowTopicHandler(),
        ToolName.UPDATE_TABLEFLOW_TOPIC: UpdateTableFlowTopicHandler(),
        ToolName.DELETE_TABLEFLOW_TOPIC: DeleteTableFlowTopicHandler(),
        ToolName.CREATE_TABLEFLOW_CATALOG_INTEGRATION: CreateTableFlowCatalogIntegrationHandler(),
        ToolName.READ_TABLEFLOW_CATALOG_INTEGRATION: ReadTableFlowCatalogIntegrationHandler(),
        ToolName.LIST_TABLEFLOW_CATALOG_INTEGRATIONS: ListTableFlowCatalogIntegrationsHandler(),
        ToolName.UPDATE_TABLEFLOW_CATALOG_INTEGRATION: UpdateTableFlowCatalogIntegrationHandler(),
        ToolName.DELETE_TABLEFLOW_CATALOG_INTEGRATION: DeleteTableFlowCatalogIntegrationHandler(),
    }
    
    @classmethod
    def create_tool_handler(cls, tool_name: ToolName) -> ToolHandler:
        """
        Get a tool handler by name.
        
        Args:
            tool_name: The name of the tool
            
        Returns:
            The tool handler instance
            
        Raises:
            ValueError: If the tool name is not recognized
        """
        if tool_name not in cls._handlers:
            raise ValueError(f"Unknown tool name: {tool_name}")
        return cls._handlers[tool_name]
    
    @classmethod
    def get_tool_configs(cls) -> list[ToolConfig]:
        """
        Get configurations for all registered tools.
        
        Returns:
            List of tool configurations
        """
        return [handler.get_tool_config() for handler in cls._handlers.values()]
    
    @classmethod
    def get_tool_config(cls, tool_name: ToolName) -> ToolConfig:
        """
        Get configuration for a specific tool.
        
        Args:
            tool_name: The name of the tool
            
        Returns:
            The tool configuration
            
        Raises:
            ValueError: If the tool name is not recognized
        """
        if tool_name not in cls._handlers:
            raise ValueError(f"Unknown tool name: {tool_name}")
        return cls._handlers[tool_name].get_tool_config()