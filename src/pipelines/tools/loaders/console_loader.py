# src/loaders/console_loader.py
from typing import List, Dict, Any, Callable
from core.logging import log_with_timestamp

def load_to_console(
    data: List[Dict[str, Any]],
    name: str = "Console Loader",
    limit: int = 5
) -> bool:
    """
    Generic console loader for debugging.
    """
    log_with_timestamp(f"[{name}] Data to load (first {limit} items):", name)
    for i, item in enumerate(data[:limit]):
        log_with_timestamp(f"  {i+1}. {item}", name, "info")
    if len(data) > limit:
        log_with_timestamp(f"  ... and {len(data) - limit} more items", name, "info")
    return True

def create_console_loader(name: str = "Console Loader", limit: int = 5) -> Callable[..., bool]:
    """Factory function to create a configured console loader."""
    def loader_func(data: List[Dict[str, Any]], *args, **kwargs) -> bool:
        log_with_timestamp(f"Running {name}", name)
        return load_to_console(data, name, limit)
    return loader_func
