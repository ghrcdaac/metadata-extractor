from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime
import os
import requests
import urllib


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

        self.get_variables_min_max(self.file_name)

    def get_variables_min_max(self, filename):
        """
        Get legacy dataset's metadata attributes from Hydro
        :param filename: file name
        :return: list longitude coordinates
        """
        hydro_base_url = "https://ghrc.nsstc.nasa.gov/hydro/es_proxy.php?esurl=_sql?sql="
        # These are seperated because we only want to encode the sql like query of the url
        hydro_query = f"SELECT * from ghrc_inv where granule_name='{self.file_name}'"
        re = requests.get(f"{hydro_base_url}{urllib.parse.quote(hydro_query)}")
        granule_info = re.json().get('hits', {}).get('hits', {})[0].get('_source', {})

        self.north = max(self.north, granule_info.get('north'))
        self.south = min(self. south, granule_info.get('south'))
        self.east = max(self.east, granule_info.get('east'))
        self.west = min(self.west, granule_info.get('west'))

        self.start_time = min(self.start_time, datetime.strptime(granule_info.get('start_date'), '%Y-%m-%d %H:%M:%S'))
        self.end_time = max(self.end_time, datetime.strptime(granule_info.get('stop_date'), '%Y-%m-%d %H:%M:%S'))

        self.format = granule_info.get('format')
        self.file_size = granule_info.get('byte_size')
        # If granule doesn't have a checksum, we fake it. If legacy dataset needs checksum, we will create
        # dedicated MDX for it.
        self.checksum = granule_info.get('checksum') if granule_info.get('checksum') is not None else \
            "2121d7505fb809fd2e93fdcf35e4ee4d"

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
