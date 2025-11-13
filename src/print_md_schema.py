#!/usr/bin/env python3
"""
Utility script to generate markdown documentation for environment variables.
"""

from typing import Any, Union, get_origin, get_args
from pydantic import BaseModel, HttpUrl
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

from env_schema import CombinedSchema


def pydantic_schema_to_markdown(model: type[BaseModel]) -> str:
    """Convert a Pydantic model to a markdown table."""
    header = (
        "| Variable | Description | Default Value | Required |\n"
        "|----------|-------------|---------------|----------|\n"
    )
    rows = extract_rows(model)
    return header + rows


def extract_rows(model: type[BaseModel]) -> str:
    """Extract and format table rows from a Pydantic model."""
    rows = []
    
    for field_name, field_info in model.model_fields.items():
        is_required = field_info.is_required()
        rows.append({
            'key': field_name,
            'field_info': field_info,
            'is_required': is_required,
        })
    
    # Sort: required first, then alphabetical
    rows.sort(key=lambda x: (not x['is_required'], x['key']))
    
    return ''.join(
        format_table_row(row['key'], row['field_info'])
        for row in rows
    )


def format_table_row(key: str, field_info: FieldInfo) -> str:
    """Format a single markdown table row."""
    # Get description
    description = field_info.description or ""
    
    # Get default value
    default_value = get_default_value(field_info)
    
    # Determine if required
    is_required = field_info.is_required()
    
    # Get type information
    type_info = get_type_info(field_info)
    
    # Combine description with type
    full_description = f"{description} ({type_info})" if description else type_info
    
    # Format row
    return f"| {key} | {full_description} | {default_value} | {'Yes' if is_required else 'No'} |\n"


def get_default_value(field_info: FieldInfo) -> str:
    """Extract and format the default value from a field."""
    if field_info.default is not PydanticUndefined and field_info.default is not None:
        if isinstance(field_info.default, str):
            return f'"{field_info.default}"'
        elif isinstance(field_info.default, HttpUrl):
            return f'"{str(field_info.default)}"'
        else:
            return str(field_info.default)
    
    if field_info.default_factory is not None:
        try:
            default = field_info.default_factory()
            if default is not None:
                if isinstance(default, str):
                    return f'"{default}"'
                return str(default)
        except Exception:
            pass
    
    return ""


def get_type_info(field_info: FieldInfo) -> str:
    """Get human-readable type information."""
    field_type = field_info.annotation
    origin = get_origin(field_type)
    args = get_args(field_type)
    
    # Handle Optional/Union types
    if origin is Union:
        non_none_args = [arg for arg in args if arg is not type(None)]
        if len(non_none_args) == 1:
            # Optional[T]
            field_type = non_none_args[0]
            origin = get_origin(field_type)
            args = get_args(field_type)
        else:
            # Union[...]
            union_types = [get_simple_type_name(arg) for arg in non_none_args]
            return f"union: {' | '.join(union_types)}"
    
    # Handle Literal (enum)
    if hasattr(field_type, '__origin__'):
        if str(field_type.__origin__) == 'typing.Literal':
            values = [str(v) for v in args]
            return f"enum: {' | '.join(values)}"
    
    # Handle List
    if origin is list:
        if args:
            element_type = get_simple_type_name(args[0])
            return f"array of {element_type}"
        return "array"
    
    # Handle basic types with constraints
    return get_type_with_constraints(field_type, field_info)


def get_type_with_constraints(field_type: Any, field_info: FieldInfo) -> str:
    """Get type name with any constraints."""
    type_name = get_simple_type_name(field_type)
    
    constraints = []
    
    # Extract constraints from metadata
    if field_info.metadata:
        for constraint in field_info.metadata:
            if hasattr(constraint, 'gt'):
                constraints.append(f"min: {constraint.gt + 1}")
            elif hasattr(constraint, 'ge'):
                constraints.append(f"min: {constraint.ge}")
            
            if hasattr(constraint, 'lt'):
                constraints.append(f"max: {constraint.lt - 1}")
            elif hasattr(constraint, 'le'):
                constraints.append(f"max: {constraint.le}")
            
            if hasattr(constraint, 'min_length'):
                constraints.append(f"min: {constraint.min_length}")
            if hasattr(constraint, 'max_length'):
                constraints.append(f"max: {constraint.max_length}")
    
    if constraints:
        return f"{type_name} ({', '.join(constraints)})"
    
    return type_name


def get_simple_type_name(field_type: Any) -> str:
    """Get simple type name without constraints."""
    if field_type is int:
        return "number"
    if field_type is str:
        return "string"
    if field_type is bool:
        return "boolean"
    if field_type is float:
        return "number"
    if field_type is HttpUrl or 'HttpUrl' in str(field_type):
        return "string (URL)"
    if isinstance(field_type, type) and issubclass(field_type, BaseModel):
        return "object"
    if hasattr(field_type, '__name__'):
        return field_type.__name__.lower()
    
    return str(field_type).replace('typing.', '').lower()


if __name__ == "__main__":
    markdown = pydantic_schema_to_markdown(CombinedSchema)
    print(markdown)