from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime
from zipfile import ZipFile
import secrets
import pathlib
import json
import os


class ExtractLegacyMetadata(ExtractASCIIMetadata):
    """
    A class to extract Legacy spatial and temporal metadata
    """
    start_time = datetime(2100, 1, 1)
    end_time = datetime(1900, 1, 1)
    north = -90.0
    south = 90.0
    east = -180.0
    west = 180.0
    format = 'ASCII'
    checksum = None
    file_size = None

    def __init__(self, file_path):
        super().__init__(file_path)

        self.file_path = file_path
        self.file_name = os.path.basename(self.file_path)
        self.lookup_zip_path = os.path.join(pathlib.Path(__file__).parent.absolute(),
                               '../src/helpers/legacy_lookups.zip')

    def get_variables_min_max(self, collection_name):
        """
        Get legacy dataset's metadata attributes from lookup zip
        :param collection_name: collection shortname used for lookup json
        """
        granule_info = None
        with ZipFile(self.lookup_zip_path) as lookup_zip:
            with lookup_zip.open(f'lookups/{collection_name}.json') as collection_lookup:
                lookup_json = json.load(collection_lookup)
                granule_info = lookup_json.get(self.file_name, {})
                print(granule_info)

        granule_wnes = granule_info.get('wnes_geometry', {})

        self.north = max(self.north, granule_wnes.get('northBoundingCoordinate'))
        self.south = min(self. south, granule_wnes.get('southBoundingCoordinate'))
        self.east = max(self.east, granule_wnes.get('eastBoundingCoordinate'))
        self.west = min(self.west, granule_wnes.get('westBoundingCoordinate'))

        temporal = [datetime.strptime(x,'%Y-%m-%dT%H:%M:%S.000Z') for x in granule_info.get('temporal', [])]
        self.start_time = min(self.start_time, min(temporal))
        self.end_time = max(self.end_time, max(temporal))

        self.format = granule_info.get('format')
        self.file_size = granule_info.get('sizeMB')
        # If granule doesn't have a checksum, we fake it. If legacy dataset needs checksum, we will create
        # dedicated MDX for it.
        self.checksum = granule_info.get('checksum') if granule_info.get('checksum') is not None else \
            secrets.token_hex(nbytes=16)

    def get_wnes_geometry(self, scale_factor=1.0, offset=0, **kwargs):
        """
        Extract the geometry from a netCDF file
        :param nc_data: netCDF data
        :param timestamp:  The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in
                                    [self.north, self.south, self.east, self.west]]
        return [self.convert_360_to_180(west), north, self.convert_360_to_180(east), south]

    def get_temporal(self, time_variable_key='time', units_variable='units', scale_factor=1.0,
                     offset=0,
                     date_format='%Y-%m-%dT%H:%M:%SZ'):
        """
        :param time_variable_key: The NetCDF variable we need to target
        :param units_variable: The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :param date_format IF specified the return type will be a string type
        :return:
        """
        start_date = self.start_time.strftime(date_format)
        stop_date = self.end_time.strftime(date_format)
        return start_date, stop_date

    def get_metadata(self, ds_short_name, format='ASCII', version='01', **kwargs):
        """

        :param ds_short_name: dataset shortname
        :param format: file format
        :param version: collection version number
        :return:
        """
        self.get_variables_min_max(ds_short_name)
        start_date, stop_date = self.get_temporal(time_variable_key='lon',
                                                  units_variable='time',
                                                  date_format='%Y-%m-%dT%H:%M:%SZ')
        data = dict()
        data['ShortName'] = ds_short_name
        data['GranuleUR'] = self.file_path.split('/')[-1]
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        gemetry_list = self.get_wnes_geometry()

        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in gemetry_list)
        # data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['SizeMBDataGranule'] = str(round(self.file_size, 2))
        # data['checksum'] = self.get_checksum()
        data['checksum'] = self.checksum
        data['DataFormat'] = self.format
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting Namdrop_raw Metadata')
    path_to_file = r'C:\Users\ecampos\Documents\granule-metadata-extractor\test\fixtures' \
                   r'\NAMMA_DROP_20060807_193132_P.dat'
    exnet = ExtractLegacyMetadata(path_to_file)
    metada = exnet.get_metadata("test")
