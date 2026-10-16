from abc import ABC, abstractmethod
import hashlib
import os

class BaseExtractor(ABC):
    """Unified base class for both lookup creators and process extractors."""
    collection_name = None

    @abstractmethod
    def process(self, file_name: str, file_path_or_obj, **kwargs) -> dict:
        """
        Core extraction logic. Must return a dictionary of metadata
        compatible with both workflows.
        """
        pass

    @staticmethod
    def get_checksum(file_path):
        """
        Reads file in 128 byte chunks to determine checksum
        :return: MD5 checksum
        """
        md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(128 * md5.block_size), b''):
                md5.update(chunk)
        return md5.hexdigest()

    @staticmethod
    def get_file_size(file_path):
        return os.path.getsize(file_path)
