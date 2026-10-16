from granule_metadata_extractor.src.metadata_extractor import MetadataExtractor
from granule_metadata_extractor.src.patch_extractor import PatchExtractor
from helpers import get_logger
from zipfile import ZipFile
from lazy_loader import get_lookup_creator_class
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
                if self.file_name in lookup_json:
                    return lookup_json.get(self.file_name, {})
                else:
                    # Fallback: If filename is not found in lookup, attempt
                    # to instantiate a modified lookup creator to get the metadata.
                    # This will be useful in applying lookup creators to
                    # ongoing datasets, so new files can still be processed without
                    # adding to the lookup dict or resorting to a processing module.
                    try:
                        creator_class = get_lookup_creator_class(collection_name)
                        patch_class = PatchExtractor
                        patch_class.process = creator_class.process
                        extractor = patch_class()
                        metadata = extractor.process(self.file_name, self.file_path)
                        if 'sizeMB' not in metadata:
                            metadata['sizeMB'] = str(round(1E-6 * extractor.get_file_size(self.file_path), 2))
                        metadata = extractor.pack_for_cumulus(metadata)
                        return metadata
                    except ValueError:
                        # Return empty and continue gracefully
                        return {}

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
                "SizeMBDataGranule": str(metadata.get("sizeMB")),
                "DataFormat": metadata.get("format", "Not Provided"),
                "VersionId": version
            }
        logger.error(f"Granule {self.file_name} not found in collection lookup "
                     f"{ds_short_name}.zip")
        return {}
