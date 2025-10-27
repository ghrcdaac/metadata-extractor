from ..src.metadata_extractor import MetadataExtractor
from ...helpers import get_logger
from zipfile import ZipFile
import pathlib
import json
import os

logger = get_logger()


class ExtractLookupMetadata(MetadataExtractor):
    """
    A class to extract lookup spatial and temporal metadata
    """
    def __init__(self, file_path):
        super().__init__(file_path)

        self.file_path = file_path
        self.file_name = os.path.basename(self.file_path)

    def get_variables_min_max(self, collection_name):
        """
        Get lookup dataset's metadata attributes from lookup zip
        :param collection_name: collection shortname used for lookup json
        """
        lookup_zip_path = os.path.join(pathlib.Path(__file__).parent.absolute(),
                               f"../src/helpers/{collection_name}.zip")
        granule_info = None
        with ZipFile(lookup_zip_path) as lookup_zip:
            with lookup_zip.open("lookup.json") as collection_lookup:
                lookup_json = json.load(collection_lookup)
                return lookup_json.get(self.file_name, {})

    def get_wnes_geometry(self, scale_factor=1.0, offset=0, **kwargs):
        """
        Only defined because it has to be for abstraction purposes
        """
        pass

    def get_temporal(self, time_variable_key="time", units_variable="units", scale_factor=1.0,
                     offset=0, date_format="%Y-%m-%dT%H:%M:%SZ"):
        """
        Only defined because it has to be for abstraction purposes
        """
        pass

    def get_metadata(self, ds_short_name, format, version="01", **kwargs):
        """
        Format lookup metadata to expected format
        :param ds_short_name: dataset shortname
        :param format: file format
        :param version: collection version number
        :return:
        """
        metadata = self.get_variables_min_max(ds_short_name)
        if metadata:
            return {
                "ShortName": ds_short_name,
                "GranuleUR": self.file_name,
                "BeginningDateTime": metadata.get("start", ""),
                "EndingDateTime": metadata.get("end", ""),
                "WestBoundingCoordinate": metadata.get("west", ""),
                "NorthBoundingCoordinate": metadata.get("north", ""),
                "EastBoundingCoordinate": metadata.get("east", ""),
                "SouthBoundingCoordinate": metadata.get("south", ""),
                "SizeMBDataGranule": str(metadata.get("size", 0)),
                "DataFormat": metadata.get("format", "Not Provided"),
                "VersionId": version
            }
        logger.error(f"Granule {self.file_name} not found in collection lookup "
                     f"{ds_short_name}.zip")
        return {}
