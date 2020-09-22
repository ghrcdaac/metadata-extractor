from ..src.extract_ascii_metadata import ExtractASCIIMetadata
import os
import pathlib
from datetime import datetime, timedelta

class ExtractSbumetimpactsASCIIMetadata(ExtractASCIIMetadata):
    """
    A class to extract sbumetimpacts 
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path

        self.fileformat = 'ASCII-csv'

        with open(self.file_path,'r') as f:
             self.file_lines = f.readlines()
        f.close()
 
        #lat and lon for *MAN.csv file is provided in datainfo sheet
        #(40.7282N, 74.0068W) 
        self.SLat, self.NLat, self.WLon, self.ELon = [40.7182, 40.7382, -74.0168, -73.9968]

        #extracting time from ascii-csv file
        [self.minTime, self.maxTime] = \
                        self.get_variables_min_max()

    def get_variables_min_max(self):
        """
        :return:
        """
        #Initializing 2 parameters
        minTime, maxTime = [datetime(2100, 1, 1),datetime(1900, 1, 1)]

        for ii in range(1,len(self.file_lines)): #skip the first line        
            #line = self.file_lines[ii].decode()
            line = self.file_lines[ii]
            tkn = line.split(";")
            cTime = datetime.strptime(line.split(";")[0], '%Y-%m-%d %H:%M:%S')
            minTime = min(minTime, cTime)
            maxTime = max(maxTime, cTime)

        return minTime, maxTime

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

    def get_metadata(self, ds_short_name, format='ASCII-csv', version='1', **kwargs):
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


if __name__ == '__main__':
    print('Extracting sbumetimpacts ascii-csv Metadata')
    path_to_file = "../../test/fixtures/IMPACTS_SBU_weatherdhs_20200216_MAN.csv"
    exnet = ExtractSbuceilimpactsMetadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)
