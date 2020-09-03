from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import json
import os
import pathlib
import gzip
import numpy as np
from datetime import datetime, timedelta
from netCDF4 import Dataset
import shutil
import tempfile
import math

#Brookhaven National Laboratory
BNL_lat = 40.875 #North
BNL_lon = -72.877 #West

class ExtractSbulidarimpactsMetadata(ExtractNetCDFMetadata):
    """
    A class to extract sbulidarimpacts 
    """
    #Brookhaven National Laboratory
    #BNL_lat = 40.875 #North
    #BNL_lon = -72.877 #West

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        #these are needed to metadata extractor

        dataset = Dataset(file_path)
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max(dataset, file_path)
        dataset.close()

    def get_variables_min_max(self, nc, file_path):
        """
        :param nc: Dataset opened
        :param file_path: file path
        :return:
        """

        #i.e., IMPACTS_SBU_dopplerlidar_20200101_rhi_BNL.nc
        dt0 = datetime.strptime(file_path.split('/')[-1].split('_')[3], '%Y%m%d') #i.e., 20200101

        start_time = dt0 + timedelta(hours=min(nc.variables['TIME'][:].flatten()).item())
        end_time = dt0 + timedelta(hours=max(nc.variables['TIME'][:].flatten()).item())

        max_range = max(nc.variables['RANGE'][:].flatten())/1000. #km
        max_elevation = max(nc.variables['ELEVATION'][:].flatten()) #degrees from ground: 0 -- parallel to ground, 90 -- vertical
        sbu_range = max_range*math.cos(math.radians(max_elevation))/111.325

        minTime, maxTime = [start_time, end_time]
        maxlat,minlat,maxlon,minlon = [BNL_lat+sbu_range, BNL_lat-sbu_range, BNL_lon+sbu_range, BNL_lon-sbu_range]

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

    def get_metadata(self, ds_short_name, format='netCDF-4', version='1', **kwargs):
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
        data['DataFormat'] = 'netCDF-4' 
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting sbulidarimpacts Metadata')
    path_to_file = "../../test/fixtures/IMPACTS_SBU_dopplerlidar_20200103_vel_az_BNL.nc"
    exnet = ExtractSbulidarimpactsMetadata(path_to_file)
    metada = exnet.get_metadata("test")
    print(metada)
