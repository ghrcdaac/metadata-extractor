from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime, timedelta
import os
import re


class ExtractFltrepepochMetadata(ExtractASCIIMetadata):
    """
    A class to extract fltrepepoch 
    """
    def __init__(self, file_path):
        super().__init__(file_path)

        self.file_name = os.path.basename(file_path)
        [self.start_time, self.end_time, self.south, self.north, self.west, self.east] = \
                        self.get_variables_min_max()

    def get_variables_min_max(self, **kwargs):
        """
        Gets start/end time and min/max lat and lon of fltrepepoch file
        :return:
        """
        # tested sample file: FlightReports_EPOCH_20170830_1458_20170831_1703.pdf  
        m = re.match(r".*.(\d{8})_(\d{4})_(\d{8})_(\d{4}).*", self.file_name)
        start_time = datetime.strptime(m.group(1)+m.group(2),'%Y%m%d%H%M')
        end_time = datetime.strptime(m.group(3)+m.group(4),'%Y%m%d%H%M')
        north = 40.
        south = 10.
        west = -130.
        east = -80.
        return start_time, end_time, south, north, west, east


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
        :return: start and end time
        """
        start_date = self.start_time.strftime(date_format)
        stop_date = self.end_time.strftime(date_format)
        return start_date, stop_date

    def get_metadata(self, ds_short_name, format='ASCII', version='01', **kwargs):
        """

        :param ds_short_name: shortname of data set
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
