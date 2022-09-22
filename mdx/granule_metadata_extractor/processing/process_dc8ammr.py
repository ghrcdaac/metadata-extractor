from ..src.extract_browse_metadata import ExtractBrowseMetadata
import os
from os import path
import json
from datetime import datetime


class ExtractDc8ammrMetadata(ExtractBrowseMetadata):
    """
    A class to extract dc8ammr
    """

    def __init__(self, file_path):
        super().__init__(file_path)

        file_name = os.path.basename(file_path)

        with open(file_path, 'rb') as f:
            self.file_lines = f.readlines()

        key = file_name.split('_')[2]
        with open(path.join(path.dirname(__file__), f"../src/helpers/dc8ammrRefData.json"),
                  'r') as fp:
            dc8ammr_loc = json.load(fp)

        self.north = dc8ammr_loc.get(key).get("NLat")
        self.south = dc8ammr_loc.get(key).get("SLat")
        self.east = dc8ammr_loc.get(key).get("ELon")
        self.west = dc8ammr_loc.get(key).get("WLon")
        self.start_time = datetime.strptime(dc8ammr_loc.get(key).get("start"),
                                            '%Y-%m-%dT%H:%M:%SZ')
        self.end_time = datetime.strptime(dc8ammr_loc.get(key).get("end"), '%Y-%m-%dT%H:%M:%SZ')

    def get_variables_min_max(self, file_name):
        """

        :param file_name: file name
        :param variable_key: The ASCII key we need to target
        :return: list longitude coordinates
        """

        pass

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

        gemetry_list = self.get_wnes_geometry()

        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in gemetry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = format
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting Dc8ammr Metadata')
    path_to_file = '/home/eddie/granule-metadata-extractor/test/fixtures' \
                   '/camex3_ammr_1998232_21ghz.gif '
    exnet = ExtractDc8ammrMetadata(path_to_file)
    metada = exnet.get_metadata("test")
