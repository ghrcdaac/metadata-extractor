from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime, timedelta
import os


class ExtractGpmkcxxgcpexMetadata(ExtractASCIIMetadata):
    """
    A class to extract gpmkcxxgcpex spatial and temporal metadata
    """
    north = 48.65
    south = 40.37
    east = -67.38
    west = -78.96

    def __init__(self, file_path):
        super().__init__(file_path)

        self.file_path = file_path
        file_name = os.path.basename(file_path)
        self.get_variables_min_max(file_name)

    def get_variables_min_max(self, file_name):
        """
        Extract values for start/end time
        :param file_name: name of file for processing
        :type file_name: str
        """
        self.start_time = datetime(year=int(file_name[11:15]),
                                   month=int(file_name[15:17]),
                                   day=int(file_name[17:19]),
                                   hour=int(file_name[19:21]),
                                   minute=int(file_name[21:23]),
                                   second=int(file_name[23:25]))
        self.end_time = self.start_time + timedelta(seconds=600) - timedelta(seconds=1)

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

    def get_metadata(self, ds_short_name, format='Not Provided', version='01', **kwargs):
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
        data['SizeMBDataGranule'] = str(
            round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = "Not Provided"
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print("Test")
