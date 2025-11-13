"""
Command-line interface argument parsing for MCP Confluent Server.
"""

import argparse
import sys
from pathlib import Path
from typing import List, Optional, Dict, Any
from dataclasses import dataclass

from dotenv import load_dotenv

from confluent_cloud.tools.tool_name import ToolName
from logger import logger
from mcp.transports.types import TransportType


@dataclass
class CLIOptions:
    """Command-line interface options"""
    env_file: Optional[str] = None
    transports: List[TransportType] = None
    allow_tools: Optional[List[str]] = None
    block_tools: Optional[List[str]] = None
    list_tools: bool = False
    disable_confluent_cloud_tools: bool = False
    kafka_config: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.transports is None:
            self.transports = [TransportType.STDIO]
        if self.kafka_config is None:
            self.kafka_config = {}


def get_package_version() -> str:
    """
    Get the package version from package metadata.
    
    Returns:
        Package version string
    """
    # Try to get version from installed package
    from importlib.metadata import version
    return version("mcp-confluent-cloud")


def parse_transport_list(value: str) -> List[TransportType]:
    """
    Parse comma-separated list of transport types.
    
    Args:
        value: Comma-separated transport types
        
    Returns:
        List of TransportType values
        
    Raises:
        argparse.ArgumentTypeError: If invalid transport types provided
    """
    # Split, trim, and filter out empty strings
    types = [t.strip() for t in value.split(",") if t.strip()]
    
    # Validate each transport type
    valid_types = set(t.value for t in TransportType)
    invalid_types = [t for t in types if t not in valid_types]
    
    if invalid_types:
        raise argparse.ArgumentTypeError(
            f"Invalid transport type(s): {', '.join(invalid_types)}. "
            f"Valid options: {', '.join(valid_types)}"
        )
    
    # Deduplicate using set and convert to TransportType
    return list(set(TransportType(t) for t in types))


def parse_tool_list(value: str) -> List[str]:
    """
    Parse comma-separated list of tool names.
    
    Args:
        value: Comma-separated list of tool names
        
    Returns:
        List of tool names
    """
    return [t.strip() for t in value.split(",") if t.strip()]


def read_file_lines(file_path: str) -> List[str]:
    """
    Read a file and return non-empty, non-comment lines.
    Lines starting with '#' are treated as comments.
    
    Args:
        file_path: Path to the file to read
        
    Returns:
        List of valid lines from the file
        
    Raises:
        FileNotFoundError: If file doesn't exist
    """
    path = Path(file_path).resolve()
    
    if not path.exists():
        raise FileNotFoundError(f"Tool list file not found: {path}")
    
    with open(path, 'r', encoding='utf-8') as f:
        lines = [
            line.strip()
            for line in f
            if line.strip() and not line.strip().startswith('#')
        ]
    
    return lines


def parse_properties_file(file_path: str) -> Dict[str, Any]:
    """
    Load configuration from a Java-style properties file.
    
    Args:
        file_path: Path to the properties file
        
    Returns:
        Dictionary of key-value pairs
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file cannot be parsed
    """
    path = Path(file_path).resolve()
    
    if not path.exists():
        raise FileNotFoundError(f"Properties file not found: {path}")
    
    properties = {}
    
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                # Strip whitespace
                line = line.strip()
                
                # Skip empty lines and comments
                if not line or line.startswith('#') or line.startswith('!'):
                    continue
                
                # Find the separator (= or :)
                if '=' in line:
                    key, value = line.split('=', 1)
                elif ':' in line:
                    key, value = line.split(':', 1)
                else:
                    # No separator found, skip
                    continue
                
                # Strip whitespace from key and value
                key = key.strip()
                value = value.strip()
                
                if key:
                    properties[key] = value
        
        return properties
    
    except Exception as e:
        raise ValueError(f"Failed to parse properties file: {e}") from e


def load_environment_variables(env_file: str) -> None:
    """
    Load environment variables from file.
    
    Args:
        env_file: Path to the environment file
        
    Raises:
        FileNotFoundError: If file doesn't exist
    """
    env_path = Path(env_file).resolve()
    
    if not env_path.exists():
        raise FileNotFoundError(f"Environment file not found: {env_path}")
    
    # Load environment variables from file
    if not load_dotenv(env_path):
        raise ValueError(f"Error loading environment variables from {env_path}")
    
    logger.info(f"Loaded environment variables from {env_path}")


def parse_cli_args() -> CLIOptions:
    """
    Parse command line arguments with strong typing.
    
    Returns:
        Parsed CLI options
    """
    parser = argparse.ArgumentParser(
        prog="mcp-confluent-cloud",
        description="Confluent MCP Server - Model Context Protocol implementation for Confluent Cloud",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {get_package_version()}"
    )
    
    parser.add_argument(
        "-e", "--env-file",
        type=str,
        metavar="<path>",
        help="Load environment variables from file"
    )
    
    parser.add_argument(
        "-k", "--kafka-config-file",
        type=str,
        metavar="<file>",
        help="Path to a properties file for configuring kafka clients"
    )
    
    parser.add_argument(
        "-t", "--transport",
        type=parse_transport_list,
        default=[TransportType.STDIO],
        metavar="<types>",
        help=f"Transport types (comma-separated list). "
             f"Valid options: {', '.join(t.value for t in TransportType)}"
    )
    
    parser.add_argument(
        "--allow-tools",
        type=str,
        metavar="<tools>",
        help="Comma-separated list of tool names to allow. "
             "If provided, takes precedence over --allow-tools-file. "
             "Allow-list is applied before block-list."
    )
    
    parser.add_argument(
        "--block-tools",
        type=str,
        metavar="<tools>",
        help="Comma-separated list of tool names to block. "
             "If provided, takes precedence over --block-tools-file. "
             "Block-list is applied after allow-list."
    )
    
    parser.add_argument(
        "--allow-tools-file",
        type=str,
        metavar="<file>",
        help="File with tool names to allow (one per line). "
             "Used only if --allow-tools is not provided. "
             "Allow-list is applied before block-list."
    )
    
    parser.add_argument(
        "--block-tools-file",
        type=str,
        metavar="<file>",
        help="File with tool names to block (one per line). "
             "Used only if --block-tools is not provided. "
             "Block-list is applied after allow-list."
    )
    
    parser.add_argument(
        "--list-tools",
        action="store_true",
        help="Print the final set of enabled tool names (with descriptions) "
             "after allow/block filtering and exit. Does not start the server."
    )
    
    parser.add_argument(
        "--disable-confluent-cloud-tools",
        action="store_true",
        help="Disable all tools that require Confluent Cloud REST APIs (cloud-only tools)."
    )
    
    try:
        args = parser.parse_args()
        
        # Load environment file if provided
        if args.env_file:
            load_environment_variables(args.env_file)
        
        # Handle allow/block tools with precedence: CLI > file > undefined
        allow_tools: Optional[List[str]] = None
        block_tools: Optional[List[str]] = None
        
        if args.allow_tools:
            allow_tools = parse_tool_list(args.allow_tools)
        elif args.allow_tools_file:
            allow_tools = read_file_lines(args.allow_tools_file)
        
        if args.block_tools:
            block_tools = parse_tool_list(args.block_tools)
        elif args.block_tools_file:
            block_tools = read_file_lines(args.block_tools_file)
        
        # Parse Kafka config file if provided
        kafka_config: Dict[str, Any] = {}
        if args.kafka_config_file:
            kafka_config = parse_properties_file(args.kafka_config_file)
        
        # Ensure transport is a list
        transports = args.transport if isinstance(args.transport, list) else [args.transport]
        
        return CLIOptions(
            env_file=args.env_file,
            transports=transports,
            allow_tools=allow_tools,
            block_tools=block_tools,
            list_tools=args.list_tools,
            disable_confluent_cloud_tools=args.disable_confluent_cloud_tools,
            kafka_config=kafka_config,
        )
    
    except (FileNotFoundError, ValueError, argparse.ArgumentTypeError) as error:
        logger.error({"error": str(error)}, "Error parsing CLI options")
        sys.exit(1)
    except SystemExit as e:
        # Let --help and --version exit normally
        if e.code == 0:
            sys.exit(0)
        raise
    except Exception as error:
        logger.error(
            {
                "error": str(error),
                "error_type": type(error).__name__,
            },
            "Error parsing CLI options"
        )
        sys.exit(1)


def get_filtered_tool_names(cli_options: CLIOptions) -> List[ToolName]:
    """
    Filter and return a sorted list of enabled ToolNames based on CLI allow/block options.
    
    This function determines which tools should be enabled for the server by applying
    the following logic:
      1. If an allow list is provided and non-empty, only those tool names present
         in the allow list (and valid) will be enabled. Invalid tool names are
         ignored with a warning.
      2. If a block list is provided and non-empty, any tool names present in the
         block list (and valid) will be removed from the enabled set. Invalid tool
         names are ignored with a warning.
      3. If neither allow nor block lists are provided, all available tools are enabled.
    
    The returned list is always sorted alphabetically.
    
    Args:
        cli_options: The parsed CLI options containing allow/block tool lists
        
    Returns:
        Alphabetically sorted list of enabled ToolNames
    """
    filtered_tool_names: List[ToolName] = list(ToolName)
    valid_tool_names = set(ToolName)
    
    # Apply allow list if provided
    if cli_options.allow_tools and len(cli_options.allow_tools) > 0:
        valid = [
            t for t in cli_options.allow_tools
            if any(tool.value == t for tool in ToolName)
        ]
        invalid = [
            t for t in cli_options.allow_tools
            if not any(tool.value == t for tool in ToolName)
        ]
        
        if invalid:
            logger.warn(
                f"Ignoring invalid tool names in allow list: {', '.join(invalid)}"
            )
        
        # Convert valid strings to ToolName enums
        filtered_tool_names = [
            tool for tool in ToolName
            if tool.value in valid
        ]
    
    # Apply block list if provided
    if cli_options.block_tools and len(cli_options.block_tools) > 0:
        valid_block = [
            t for t in cli_options.block_tools
            if any(tool.value == t for tool in ToolName)
        ]
        invalid_block = [
            t for t in cli_options.block_tools
            if not any(tool.value == t for tool in ToolName)
        ]
        
        if invalid_block:
            logger.warn(
                f"Ignoring invalid tool names in block list: {', '.join(invalid_block)}"
            )
        
        # Filter out blocked tools
        filtered_tool_names = [
            tool for tool in filtered_tool_names
            if tool.value not in valid_block
        ]
    
    # Deduplicate and sort
    deduped = sorted(set(filtered_tool_names), key=lambda x: x.value)
    
    # Log if no filters applied
    if (not cli_options.allow_tools or len(cli_options.allow_tools) == 0) and \
       (not cli_options.block_tools or len(cli_options.block_tools) == 0):
        logger.info(
            "No allow/block tool lists provided; all tools are enabled by default."
        )
    
    return deduped