from granule_metadata_extractor.src.metadata_extractor import MetadataExtractor
from zipfile import ZipFile
import pathlib
import json
import os


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
                               f"../src/helpers/parprbimpacts.zip")
        granule_info = None
        with ZipFile(lookup_zip_path) as lookup_zip:
            with lookup_zip.open("lookup.json") as collection_lookup:
                lookup_json = json.load(collection_lookup)
                return lookup_json.get(self.file_name, {})


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
                "SizeMBDataGranule": str(metadata.get("sizeMB", 0)),
                "DataFormat": metadata.get("format", "Not Provided"),
                "VersionId": version
            }
        print(f"Granule {self.filename} not found in collection lookup "
                f"{ds_short_name}.zip")
        return {}
