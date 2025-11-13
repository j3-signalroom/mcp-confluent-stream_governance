from typing import Any
from pydantic import BaseModel, Field
import json
import logging
import asyncio
from mcp.types import CallToolResult

from client_manager import ClientManager
from base_tools import BaseToolHandler, ToolConfig
from tool_name import ToolName

logger = logging.getLogger(__name__)


class ListSchemasArguments(BaseModel):
    """Arguments for listing schemas"""
    latest_only: bool = Field(
        default=True,
        description="If true, only return the latest version of each schema."
    )
    subject_prefix: str | None = Field(
        default=None,
        description="The prefix of the subject to list schemas for."
    )
    deleted: bool = Field(
        default=False,
        description="List deleted schemas. (Only used if latestOnly is false)"
    )


class ListSchemasHandler(BaseToolHandler):
    """Handler for listing schemas in the Schema Registry"""
    
    async def handle(
        self,
        client_manager: ClientManager,
        tool_arguments: dict[str, Any] | None = None,
        session_id: str | None = None,
    ) -> CallToolResult:
        """
        List all schemas in the Schema Registry.
        
        Args:
            client_manager: Manager for API clients
            tool_arguments: Optional arguments for filtering
            session_id: Optional session identifier
            
        Returns:
            Result containing the schema listing
        """
        try:
            # Parse and validate arguments
            args = ListSchemasArguments.model_validate(tool_arguments or {})
        except Exception as e:
            return self.create_response(
                f"Invalid arguments: {str(e)}",
                is_error=True
            )
        
        logger.debug(
            "ListSchemasHandler.handle called with arguments: latest_only=%s, subject_prefix=%s, deleted=%s",
            args.latest_only,
            args.subject_prefix,
            args.deleted
        )
        
        registry = client_manager.get_schema_registry_client()
        
        try:
            # Get all subjects
            subjects = await registry.get_all_subjects()
            logger.debug("Fetched all subjects from registry: count=%d", len(subjects))
            
            # Filter by prefix if provided
            if args.subject_prefix:
                subjects = [s for s in subjects if s.startswith(args.subject_prefix)]
                logger.debug(
                    "Filtered subjects by prefix '%s': count=%d",
                    args.subject_prefix,
                    len(subjects)
                )
            
            result: dict[str, Any] = {}
            
            # Process each subject
            for subject in subjects:
                if args.latest_only:
                    # Get only the latest version
                    try:
                        latest = await registry.get_latest_schema_metadata(subject)
                        logger.debug(
                            "Fetched latest schema metadata for subject '%s'",
                            subject
                        )
                        result[subject] = {
                            "version": latest.version,
                            "id": latest.id,
                            "schemaType": latest.schema_type,
                            "schema": latest.schema,
                        }
                    except Exception as err:
                        logger.warning(
                            "Failed to fetch latest schema metadata for subject '%s': %s",
                            subject,
                            str(err)
                        )
                        result[subject] = {
                            "error": str(err)
                        }
                else:
                    # Get all versions
                    try:
                        versions = await registry.get_all_versions(subject)
                        logger.debug(
                            "Fetched all schema versions for subject '%s': versions=%s",
                            subject,
                            versions
                        )
                        result[subject] = []
                        
                        # Fetch all versions in parallel
                        async def fetch_version(version: int) -> dict[str, Any]:
                            """Fetch metadata for a specific version"""
                            try:
                                schema = await registry.get_schema_metadata(
                                    subject,
                                    version,
                                    args.deleted
                                )
                                logger.debug(
                                    "Fetched schema metadata for subject '%s' version %d",
                                    subject,
                                    version
                                )
                                return {
                                    "version": schema.version,
                                    "id": schema.id,
                                    "schemaType": schema.schema_type,
                                    "schema": schema.schema,
                                }
                            except Exception as err:
                                logger.warning(
                                    "Failed to fetch schema metadata for subject '%s' version %d: %s",
                                    subject,
                                    version,
                                    str(err)
                                )
                                return {
                                    "version": version,
                                    "error": str(err)
                                }
                        
                        # Gather all version results in parallel
                        version_results = await asyncio.gather(
                            *[fetch_version(version) for version in versions]
                        )
                        result[subject] = version_results
                        
                    except Exception as err:
                        logger.warning(
                            "Failed to fetch all versions for subject '%s': %s",
                            subject,
                            str(err)
                        )
                        result[subject] = {
                            "error": str(err)
                        }
            
            logger.info(
                "Returning schema listing result: subjects=%d",
                len(result)
            )
            return self.create_response(json.dumps(result, indent=2))
            
        except Exception as error:
            logger.error("Failed to list schemas: %s", str(error))
            return self.create_response(
                f"Failed to list schemas: {str(error)}",
                is_error=True
            )
    
    def get_tool_config(self) -> ToolConfig:
        """Get the tool configuration"""
        return ToolConfig(
            name=ToolName.LIST_SCHEMAS,
            description="List all schemas in the Schema Registry.",
            inputSchema=ListSchemasArguments.model_json_schema()
        )
    
    def get_required_env_vars(self) -> list[str]:
        """Get required environment variables"""
        return [
            "SCHEMA_REGISTRY_ENDPOINT",
            "SCHEMA_REGISTRY_API_KEY",
            "SCHEMA_REGISTRY_API_SECRET",
        ]