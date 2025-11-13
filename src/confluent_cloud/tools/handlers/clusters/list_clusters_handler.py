from typing import Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import os
import logging

from client_manager import ClientManager
from mcp.types import CallToolResult
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName

logger = logging.getLogger(__name__)


class ListClustersArguments(BaseModel):
    """Arguments for listing clusters"""
    base_url: HttpUrl | None = Field(
        default=None,
        description="The base URL of the Confluent Cloud REST API."
    )
    environment_id: str | None = Field(
        default=None,
        description="The environment ID to filter clusters by"
    )
    
    @field_validator('base_url', mode='before')
    @classmethod
    def set_default_base_url(cls, v: str | None) -> str | None:
        """Set default base_url from environment if not provided"""
        if v is None or v == "":
            return os.getenv("CONFLUENT_CLOUD_REST_ENDPOINT")
        return v if isinstance(v, str) else None


# Response validation schemas
class ClusterMetadata(BaseModel):
    """Cluster metadata"""
    created_at: str
    resource_name: str
    self: str
    updated_at: str


class ClusterEnvironment(BaseModel):
    """Cluster environment reference"""
    id: str
    related: str
    resource_name: str


class ClusterConfig(BaseModel):
    """Cluster configuration"""
    cku: int | None = None
    kind: str
    zones: list[str] | None = None


class ClusterSpec(BaseModel):
    """Cluster specification"""
    api_endpoint: str
    availability: str
    cloud: str
    config: ClusterConfig
    display_name: str
    environment: ClusterEnvironment
    http_endpoint: str
    kafka_bootstrap_endpoint: str
    region: str


class ClusterStatus(BaseModel):
    """Cluster status"""
    cku: int | None = None
    phase: str


class Cluster(BaseModel):
    """Full cluster response from API"""
    api_version: str
    id: str
    kind: str
    metadata: ClusterMetadata
    spec: ClusterSpec
    status: ClusterStatus


class ClusterSummary(BaseModel):
    """Simplified cluster summary for output"""
    id: str
    name: str
    availability: str
    cloud: str
    region: str
    environment_id: str
    status: str
    cku: int
    endpoints: dict[str, str]
    config: dict[str, Any]


class ListClustersHandler(BaseToolHandler):
    """Handler for listing Kafka clusters in Confluent Cloud"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        Get all clusters in the Confluent Cloud environment.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Optional arguments for filtering and configuration
            session_id: Optional session identifier
            
        Returns:
            Result containing the list of clusters
        """
        try:
            # Parse and validate arguments
            args = ListClustersArguments.model_validate(tool_arguments or {})
        except Exception as e:
            return self.create_response(
                f"Invalid arguments: {str(e)}",
                is_error=True
            )
        
        try:
            # Update base URL if provided
            if args.base_url is not None and str(args.base_url) != "":
                client_manager.set_confluent_cloud_rest_endpoint(str(args.base_url))
            
            # Get the Confluent Cloud REST client
            client = client_manager.get_confluent_cloud_rest_client()
            
            # Prepare query parameters
            environment_id = (
                args.environment_id or 
                os.getenv("KAFKA_ENV_ID") or 
                ""
            )
            
            params = {
                "environment": environment_id,
                "page_size": 100,
            }
            
            # Make the GET API call with query parameters
            response = await client.get(
                "/cmk/v2/clusters",
                params=params
            )
            
            if response.status_code >= 400:
                error_text = await response.text()
                logger.error("API Error: %s", error_text)
                return self.create_response(
                    f"Failed to fetch clusters: {error_text}",
                    is_error=True,
                    meta={"error": error_text}
                )
            
            # Parse response
            response_data = await response.json()
            
            # Validate the response structure
            if not response_data or not isinstance(response_data, dict):
                return self.create_response(
                    "Invalid response format: response is not an object",
                    is_error=True,
                    meta={"response": response_data}
                )
            
            if "data" not in response_data or not isinstance(response_data["data"], list):
                return self.create_response(
                    "Invalid response format: missing or invalid data array",
                    is_error=True,
                    meta={"response": response_data}
                )
            
            # Validate and transform clusters
            clusters: list[ClusterSummary] = []
            for cluster_data in response_data["data"]:
                try:
                    # Validate against schema
                    validated_cluster = Cluster.model_validate(cluster_data)
                    
                    # Transform to summary format
                    cluster_summary = ClusterSummary(
                        id=validated_cluster.id,
                        name=validated_cluster.spec.display_name,
                        availability=validated_cluster.spec.availability,
                        cloud=validated_cluster.spec.cloud,
                        region=validated_cluster.spec.region,
                        environment_id=validated_cluster.spec.environment.id,
                        status=validated_cluster.status.phase,
                        cku=(
                            validated_cluster.status.cku or
                            validated_cluster.spec.config.cku or
                            0
                        ),
                        endpoints={
                            "http": validated_cluster.spec.http_endpoint,
                            "bootstrap": validated_cluster.spec.kafka_bootstrap_endpoint,
                        },
                        config={
                            "kind": validated_cluster.spec.config.kind,
                            "zones": validated_cluster.spec.config.zones or [],
                        }
                    )
                    clusters.append(cluster_summary)
                    
                except Exception as validation_error:
                    logger.error("Cluster validation error: %s", validation_error)
                    raise ValueError(
                        f"Invalid cluster data: {str(validation_error)}"
                    )
            
            # Format cluster details for display
            cluster_details = "\n".join([
                f"""
Cluster: {cluster.name}
  ID: {cluster.id}
  Environment ID: {cluster.environment_id}
  Status: {cluster.status}
  Availability: {cluster.availability}
  Cloud: {cluster.cloud}
  Region: {cluster.region}
  CKU: {cluster.cku}
  Endpoints:
    HTTP: {cluster.endpoints['http']}
    Bootstrap: {cluster.endpoints['bootstrap']}
  Config:
    Kind: {cluster.config['kind']}
    Zones: {', '.join(cluster.config['zones'])}
"""
                for cluster in clusters
            ])
            
            # Prepare metadata
            total_size = response_data.get("metadata", {}).get("total_size")
            
            return self.create_response(
                f"Successfully retrieved {len(clusters)} clusters:\n{cluster_details}",
                is_error=False,
                meta={
                    "clusters": [cluster.model_dump() for cluster in clusters],
                    "total": total_size
                }
            )
            
        except Exception as error:
            logger.error("Error in ListClustersHandler: %s", error)
            return self.create_response(
                f"Failed to fetch clusters: {str(error)}",
                is_error=True,
                meta={"error": str(error)}
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.LIST_CLUSTERS,
            description="Get all clusters in the Confluent Cloud environment",
            inputSchema=ListClustersArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return ["CONFLUENT_CLOUD_API_KEY", "CONFLUENT_CLOUD_API_SECRET"]
    
    def is_confluent_cloud_only(self) -> bool:
        """This tool is only for Confluent Cloud"""
        return True