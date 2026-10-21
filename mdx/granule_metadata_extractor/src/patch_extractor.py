"""Empty class for patching in other classes."""
from .base_extractor import BaseExtractor
from datetime import datetime
from typing import Any

DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

class PatchExtractor(BaseExtractor):

    def process(self, file_name, file_path_or_obj, **kwargs) -> dict:
        pass

    @staticmethod
    def pack_for_cumulus(metadata: dict[str, Any]):
        for field in ['start', 'end']:
            if isinstance(metadata[field], datetime):
                metadata[field] = metadata[field].strftime(DATETIME_FORMAT)
        for field in ['north', 'east', 'south', 'west']:
            metadata[field] = f"{metadata[field]:0.3f}"
        return metadata