POSTING_PROVIDER_KEYS = {
    "id",
    "name",
    "enabled",
    "host",
    "port",
    "ssl",
    "username",
    "password",
    "connections",
    "max_connections",
    "priority_up_to_gb",
}


def sanitize_posting_provider_items(provider_items):
    if not isinstance(provider_items, list):
        return provider_items
    sanitized = []
    for item in provider_items:
        if isinstance(item, dict):
            sanitized.append({key: value for key, value in item.items() if key in POSTING_PROVIDER_KEYS})
        else:
            sanitized.append(item)
    return sanitized
