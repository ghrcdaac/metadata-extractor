from ..src.extract_ascii_metadata import ExtractASCIIMetadata
import os
import pathlib
from datetime import datetime, timedelta

class ExtractNavghepochMetadata(ExtractASCIIMetadata):
    """
    A class to extract navghepoch 
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path

        self.fileformat = 'CSV'

        with open(self.file_path,'r') as f:
             self.file_lines = f.readlines()
        f.close()
 
        #extracting lat, lon, and time from csv file
        [self.SLat, self.NLat, self.WLon, self.ELon, self.minTime, self.maxTime] = \
                        self.get_variables_min_max()

    def get_variables_min_max(self):
        """
        :return:
        """
        time_arr = []
        lat_arr = []
        lon_arr = []
        for line in self.file_lines[1:]: #skip the first line
            #IWG1,2017-07-27T13:21:52.000Z,34.9205974024,-117.87497554,...
            tkn = line.split(',')
            time_arr.append(datetime.strptime(tkn[1],'%Y-%m-%dT%H:%M:%S.%fZ'))
            lat_arr.append(float(tkn[2]))
            lon_arr.append(float(tkn[3]))
        return min(lat_arr), max(lat_arr), min(lon_arr), max(lon_arr), min(time_arr), max(time_arr)

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry from a GIF file
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in
                                    [self.NLat, self.SLat, self.ELon, self.WLon]]
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
        start_date = self.minTime.strftime(date_format)
        stop_date = self.maxTime.strftime(date_format)
        return start_date, stop_date

    def get_metadata(self, ds_short_name, format='CSV', version='1', **kwargs):
        """
        :param ds_short_name:
        :param time_variable_key:
        :param lon_variable_key:
        :param lat_variable_key:
        :param time_units:
        :param format:
        :return:
        """
        data = dict()
        data['GranuleUR'] = granule_name = os.path.basename(self.file_path)
        start_date, stop_date = self.get_temporal()
        data['ShortName'] = ds_short_name
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        geometry_list = self.get_wnes_geometry()
        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in geometry_list)
        data['checksum'] = self.get_checksum()
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['DataFormat'] = self.fileformat
        data['VersionId'] = version
        return data
