import importlib
import sys
import json
from pathlib import Path
from helpers import get_logger

logger = get_logger()

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
        # Can merge with other registries if need be, or keep them separate
    return _MAP_CACHE

def lazy_load_lookup_module(filename):
    module_base_path = Path(f"mdx/granule_metadata_extractor/src/helpers/creators")
    module_name = (module_base_path / filename).stem
    import_path = str(module_base_path).replace("/", ".") + '.' + module_name
    try:
        module = importlib.import_module(import_path)
    except ImportError as e:
        logger.warning(f"Failed to lazy-load module at {import_path}")
        logger.warning(e)
        raise e
    return module

def get_lookup_creator_class(ds_short_name: str):
    """
    Lazily import and return the extractor class using the JSON map.
    """
    if ds_short_name in _LOADED_CACHE:
        return _LOADED_CACHE[ds_short_name]

    registry = _load_registry()
    module_type, filename = registry.get(ds_short_name, [None, None])

    if module_type is None:
        # Module not found in registry
        # Attempt to find a matching one anyway
        # Assume lookup module named {collection_name}.py
        filename = f"{ds_short_name}.py"
        module = lazy_load_lookup_module(filename)

    # Import the specific module dynamically
    elif module_type == "lookup":
        module = lazy_load_lookup_module(filename)
    else:
        raise ValueError(f"Unknown module type: {module_type}")

    if module is None:
        raise ValueError(f"Failed to load lookup module for {ds_short_name}")

    lookup_class = module.MDXProcessing
    if not hasattr(lookup_class, 'process'):
        # Doesn't have what we need; abort
        raise ImportError(f"Could not find valid extractor class in {import_path}")

    _LOADED_CACHE[ds_short_name] = lookup_class
    return lookup_class
