"""
Utility functions for parameter validation with stronger typing.
"""

from typing import Any, TypeVar, Optional, Union, overload

from env import env


T = TypeVar('T')


@overload
def get_ensured_param(
    env_var_name: str,
    error_message: str,
    param: None = None,
) -> Any: ...

@overload
def get_ensured_param(
    env_var_name: str,
    error_message: str,
    param: T,
) -> T: ...


def get_ensured_param(
    env_var_name: str,
    error_message: str,
    param: Optional[T] = None,
) -> Union[T, Any]:
    """
    Ensure a parameter exists either from the provided value or environment variable.
    Favors the provided param over the environment variable when truthy.
    
    Args:
        env_var_name: The name of the environment variable to check
        error_message: The error message to raise if neither parameter nor environment variable exists
        param: Optional parameter value to use instead of environment variable
        
    Returns:
        The parameter value or environment variable value
        
    Raises:
        ValueError: When neither parameter nor environment variable exists
    """
    # Favor provided param over environment variable when truthy
    final_param = param if param else getattr(env, env_var_name, None)
    
    if not final_param:
        raise ValueError(error_message)
    
    return final_param


# Alternative: More explicit version with type narrowing
def get_ensured_param_typed(
    env_var_name: str,
    error_message: str,
    param: Optional[T] = None,
    default: Optional[T] = None,
) -> T:
    """
    Ensure a parameter exists with optional default fallback.
    
    Priority order:
    1. Provided param (if truthy)
    2. Environment variable
    3. Default value (if provided)
    4. Raise error
    
    Args:
        env_var_name: The name of the environment variable to check
        error_message: The error message to raise if no value found
        param: Optional parameter value to use
        default: Optional default value as final fallback
        
    Returns:
        The resolved parameter value
        
    Raises:
        ValueError: When no value is found from any source
    """
    # Check param first
    if param:
        return param
    
    # Check environment variable
    env_value = getattr(env, env_var_name, None)
    if env_value:
        return env_value
    
    # Check default
    if default is not None:
        return default
    
    # Nothing found, raise error
    raise ValueError(error_message)