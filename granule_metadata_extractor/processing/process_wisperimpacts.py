from ..src.extract_ascii_metadata import ExtractASCIIMetadata
import os
import numpy as np
from datetime import datetime, timedelta
import json

class ExtractWisperimpactsMetadata(ExtractASCIIMetadata):
    """
    A class to extract wisperimpacts 
    """

    def __init__(self, file_path):
        self.file_path = file_path
        # these are needed to metadata extractor
        self.fileformat = 'ASCII'

        gps_path = '../src/helpers/P3_Nav_impacts.json'
        with open(os.path.join(os.path.dirname(__file__),gps_path), 'r') as fp:
             P3_Nav = json.load(fp)
        self.nav_time = np.array([datetime.strptime(x,'%Y%m%d%H%M%S') for x in P3_Nav['time'][:]])
        self.nav_lat = np.array(P3_Nav['lat'][:])
        self.nav_lon = np.array(P3_Nav['lon'][:])

        # extracting time and space metadata for png file
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
            self.get_variables_min_max()

    def get_variables_min_max(self):
        """
        :return:
        """
        #sample file:
        #IMPACTS-WISPER-CWC_P3_20200118_R0_SF01.ict
        date_str = self.file_path.split('/')[-1].split('_')[2] #i.e., 20200118
        flight_date = datetime.strptime(date_str,'%Y%m%d')  

        with open(self.file_path,'rb') as f:
             lines = f.readlines()

        num_header_lines = int(lines[0].decode().split(',')[0])

        utc = []
        for line in lines[num_header_lines:]:
            sec = float(line.decode().split(',')[0])
            utc.append(flight_date+timedelta(seconds=sec))
 
        minTime, maxTime = [min(utc), max(utc)] 
        idx = np.where((self.nav_time >= minTime) & (self.nav_time <= maxTime))[0]
        maxlat, minlat, maxlon, minlon = [self.nav_lat[idx].max(),
                                          self.nav_lat[idx].min(),
                                          self.nav_lon[idx].max(),
                                          self.nav_lon[idx].min()]

        return minTime, maxTime, minlat, maxlat, minlon, maxlon


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

    def get_metadata(self, ds_short_name, format='PNG', version='1', **kwargs):
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
