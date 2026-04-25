"""Utilities for sanitizing and redacting sensitive data from logs and responses."""

import re
from typing import Any, Dict


# Patterns to match sensitive data
_SENSITIVE_PATTERNS = [
    # Passwords and credentials
    (r'password["\']?\s*[:=]\s*["\']?([^"\'\s,;]+)["\']?', 'password'),
    (r'passwd["\']?\s*[:=]\s*["\']?([^"\'\s,;]+)["\']?', 'password'),
    (r'pwd["\']?\s*[:=]\s*["\']?([^"\'\s,;]+)["\']?', 'password'),
    
    # API keys and tokens
    (r'api[_-]?key["\']?\s*[:=]\s*["\']?([a-zA-Z0-9_\-\.]{8,})["\']?', 'api_key'),
    (r'token["\']?\s*[:=]\s*["\']?([a-zA-Z0-9_\-\.]{8,})["\']?', 'token'),
    (r'auth[_-]?token["\']?\s*[:=]\s*["\']?([a-zA-Z0-9_\-\.]{8,})["\']?', 'token'),
    (r'bearer\s+([a-zA-Z0-9_\-\.]{8,})', 'bearer_token'),
    
    # Connection strings and URLs with credentials
    (r'(?:mysql|postgres|mongodb|redis)://[^/]*:([^@/]+)@', 'password_in_url'),
    
    # Email addresses (optional redaction)
    # (r'([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+)', 'email'),
    
    # File paths that might contain usernames (optional)
    # (r'/home/([a-zA-Z0-9_\-]+)/', 'username_in_path'),
]


def redact_sensitive_data(text: str, replace_with: str = "***REDACTED***") -> str:
    """
    Remove sensitive values from text (logs, error messages, etc).
    
    Args:
        text: Input text to sanitize
        replace_with: Replacement string for redacted values
        
    Returns:
        Text with sensitive values replaced
    """
    if not isinstance(text, str):
        return str(text)
    
    result = text
    for pattern, field_type in _SENSITIVE_PATTERNS:
        result = re.sub(pattern, replace_with, result, flags=re.IGNORECASE)
    
    return result


def redact_cli_command(cmd_list: list, unsafe_args: set = None) -> str:
    """
    Redact sensitive arguments from a command line and return as string.
    
    Args:
        cmd_list: Command as a list (e.g., ['rar', 'a', '-p123', ...])
        unsafe_args: Set of argument names/prefixes to redact (e.g., {'-p', '--password'})
        
    Returns:
        Redacted command as string
    """
    if unsafe_args is None:
        unsafe_args = {'-p', '--password', '--user', '--username', '-u', '--token', '--api-key'}
    
    redacted = []
    skip_next = False
    
    for i, arg in enumerate(cmd_list):
        if skip_next:
            redacted.append(f"{arg.split('=')[0]}=***REDACTED***" if '=' in arg else "***REDACTED***")
            skip_next = False
            continue
        
        # Check for unsafe flag with = separator
        if '=' in arg:
            flag, value = arg.split('=', 1)
            if any(flag.startswith(u) for u in unsafe_args):
                redacted.append(f"{flag}=***REDACTED***")
                continue
        
        # Check for unsafe flag (next arg is value)
        if any(arg.startswith(u) for u in unsafe_args):
            if '=' not in arg:
                skip_next = True
            redacted.append(arg.split('=')[0] + "=***REDACTED***" if '=' in arg else arg)
            continue
        
        redacted.append(arg)
    
    return " ".join(redacted)


def sanitize_provider_param(value: str, param_type: str = "general") -> str:
    """
    Validate and sanitize user input for posting provider parameters.
    
    Args:
        value: The parameter value
        param_type: Type of validation - 'email', 'group', 'password', 'general'
        
    Returns:
        Sanitized value
        
    Raises:
        ValueError: If value fails validation
    """
    if not isinstance(value, str):
        value = str(value)
    
    value = value.strip()
    
    if param_type == "email":
        # Strict email format
        if not re.match(r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$", value.lower()):
            raise ValueError(f"Invalid email format: {value}")
    
    elif param_type == "group":
        # Usenet group names: lowercase, dots, hyphens only
        if not re.match(r"^[a-z0-9.\-]+$", value.lower()):
            raise ValueError(f"Invalid usenet group name: {value}")
        if len(value) > 256:
            raise ValueError(f"Group name too long: {len(value)} chars")
    
    elif param_type == "password":
        # Reject null bytes and control characters
        if any(ord(c) < 32 for c in value):
            raise ValueError("Password contains control characters")
        if len(value) > 128:
            raise ValueError(f"Password too long: {len(value)} chars (max 128)")
        if len(value) < 1:
            raise ValueError("Password cannot be empty")
    
    elif param_type == "integer_port":
        # Port number validation
        try:
            port = int(value)
            if port < 1 or port > 65535:
                raise ValueError(f"Port must be 1-65535: {port}")
            return str(port)
        except (ValueError, TypeError):
            raise ValueError(f"Invalid port number: {value}")
    
    elif param_type == "general":
        # Basic sanitation: no null bytes, not too long
        if any(ord(c) < 32 and c not in '\t\n\r' for c in value):
            raise ValueError("Value contains invalid control characters")
        if len(value) > 1024:
            raise ValueError(f"Value too long: {len(value)} chars")
    
    return value


def safe_dict_repr(obj: Dict[str, Any], redact: bool = True) -> Dict[str, Any]:
    """
    Create a safe dictionary representation with sensitive values redacted.
    
    Args:
        obj: Dictionary to sanitize
        redact: Whether to redact sensitive values
        
    Returns:
        Safe dictionary copy
    """
    safe = {}
    sensitive_keys = {'password', 'passwd', 'pwd', 'token', 'api_key', 'secret', 'key'}
    
    for k, v in obj.items():
        if redact and any(s in k.lower() for s in sensitive_keys):
            safe[k] = "***REDACTED***"
        elif isinstance(v, dict):
            safe[k] = safe_dict_repr(v, redact)
        elif isinstance(v, (list, tuple)):
            safe[k] = [safe_dict_repr(item, redact) if isinstance(item, dict) else item for item in v]
        else:
            safe[k] = v
    
    return safe
