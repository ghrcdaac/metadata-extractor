import os
import re
from datetime import datetime, timedelta

from ..src.extract_ascii_metadata import ExtractASCIIMetadata


class ExtractAces1EfmMetadata(ExtractASCIIMetadata):
    """
    A class to extract aces1efm
    """
    start_time = datetime(2100, 1, 1)
    end_time = datetime(1900, 1, 1)
    north = 26.0
    south = 23.0
    east = -81.0
    west = -85.0
    format = "Binary"

    def __init__(self, file_path):
        super().__init__(file_path)
        self.get_variables_min_max(os.path.basename(file_path))

    def get_variables_min_max(self, file_name):
        """
        :param file_name: file name
        :param variable_key: The ASCII key we need to target
        :return: list longitude coordinates
        """
        matches = re.search("aces1efm_(\\d{4}).(\\d+)_v2.50.tar", file_name)
        year, day = int(matches[1]), int(matches[2])
        self.start_time = datetime(year, 1, 1) + timedelta(days=day - 1)
        self.end_time = self.start_time + timedelta(days=1) - timedelta(seconds=1)

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

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
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

    def get_metadata(self, ds_short_name, format='ASCII', version='01'):
        """
        :param ds_short_name:
        :param time_variable_key:
        :param lon_variable_key:
        :param lat_variable_key:
        :param time_units:
        :param format:
        :return:
        """
        start_date, stop_date = self.get_temporal(time_variable_key='lon',
                                                  units_variable='time',
                                                  date_format='%Y-%m-%dT%H:%M:%SZ')
        data = dict()
        data['ShortName'] = ds_short_name
        data['GranuleUR'] = self.file_path.split('/')[-1]
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        geometry_list = self.get_wnes_geometry()

        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in geometry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = self.format
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting Namdrop_raw Metadata')
    # path_to_file = r'C:\Users\ecampos\Documents\granule-metadata-extractor\test\fixtures' \
    #                r'\NAMMA_DROP_20060807_193132_P.dat'
    # exnet = ExtractNamdrop_rawMetadata(path_to_file)
    # metada = exnet.get_metadata("test")