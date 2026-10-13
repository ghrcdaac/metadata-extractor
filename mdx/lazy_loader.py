import importlib
import sys
import json
from pathlib import Path

_MAP_CACHE = None
_LOADED_CACHE = {}

LOOKUP_CREATORS_REGISTRY = "granule_metadata_extractor/src/helpers/creators/creators.json"

def _load_registry():
    global _MAP_CACHE
    if _MAP_CACHE is None:
        lookup_creators_path = Path(__file__).parent / LOOKUP_CREATORS_REGISTRY
        with open(lookup_creators_path, "r") as f:
            lookup_creators_map = json.load(f)
            lookup_creators_map = {short_name: ["lookup", filename] for short_name, filename
                                   in lookup_creators_map.items()}
            _MAP_CACHE = lookup_creators_map
        # Can merge with other registries if need be
    return _MAP_CACHE

def get_extractor_class(ds_short_name: str):
    """
    Lazily import and return the extractor class using the JSON map.
    """
    if ds_short_name in _LOADED_CACHE:
        return _LOADED_CACHE[ds_short_name]

    registry = _load_registry()
    module_name, filename = registry.get(ds_short_name, [None, None])

    if not module_name:
        raise ValueError(f"No extractor mapping found for collection: {ds_short_name}")

    # Import the specific module dynamically
    elif module_name == "lookup":
        module_path = f"mdx.granule_metadata_extractor.src.helpers.creators.{module_name}"
    module = importlib.import_module(module_path)

    # Extract the class (assuming standard attribute or searching attributes)
    extractor_class = None
    for attr_name in dir(module):
        obj = getattr(module, attr_name)
        # Find classes defined in the module that have a process method
        if hasattr(obj, "process") and isinstance(obj, type):
            extractor_class = obj
            break

    if not extractor_class:
        raise ImportError(f"Could not find valid extractor class in {module_path}")

    _LOADED_CACHE[ds_short_name] = extractor_class
    return extractor_class
