from typing import Dict, Any, Optional, Callable


def get(obj: Dict[str, Any], key: str, transformer: Optional[Callable[[Any], Any]] = None) -> Any:
    if key not in obj:
        return None
    value = obj[key]
    if transformer:
        return transformer(value)
    return value
