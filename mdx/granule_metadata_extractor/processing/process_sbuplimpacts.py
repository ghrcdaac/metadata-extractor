from ..src.extract_ascii_metadata import ExtractASCIIMetadata
import os
import pathlib
from datetime import datetime, timedelta

class ExtractSbuplimpactsMetadata(ExtractASCIIMetadata):
    """
    A class to extract sbuplimpacts 
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path

        self.fileformat = 'CSV'
        self.site_lat_2020 = 40.89
        self.site_lon_2020 = -73.128
        self.site_lat_2022 = 40.89712
        self.site_lon_2022 = -73.12771

        with open(self.file_path,'r') as f:
             self.file_lines = f.readlines()
        f.close()

        filename = self.file_path.split('/')[-1]
        if 'pluvio_2020' in filename:
           site_lat = self.site_lat_2020 
           site_lon = self.site_lon_2020 
        else: #pluvio_2022
           site_lat = self.site_lat_2022 
           site_lon = self.site_lon_2022 
 
        self.SLat, self.NLat, self.WLon, self.ELon = [site_lat-0.01,
                                                      site_lat+0.01,
                                                      site_lon-0.01,
                                                      site_lon+0.01]
        #extracting time from csv file
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
            dtstr = ''.join(self.file_lines[ii].split(',')[0:2])
            cTime = datetime.strptime(dtstr, '%Y%m%d%H:%M:%S')
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


if __name__ == '__main__':
    print('Extracting sbuplimpacts Metadata')
    path_to_file = "../../test/fixtures/IMPACTS_SBU_pluvio_20200114.csv"
    exnet = ExtractSbuplimpactsMetadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)
