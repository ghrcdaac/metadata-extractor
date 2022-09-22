from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import json
from os import path
from datetime import datetime, timedelta
import re
import numpy as np 
from netCDF4 import Dataset


class ExtractNcsusndimpactsMetadata(ExtractNetCDFMetadata):
    """
    A class to extract NetCDF metadata 
    """
    metadata = {}
    max_lat = -90.0
    min_lat = 90.0
    max_lon = -180.0
    min_lon = 180.0
    minTime = datetime(2100, 1, 1)
    maxTime = datetime(1900, 1, 1)

    #Raleigh, NC 
    #Lat: 35+46/60.=35.766666666666666
    #Lon: -1.*(78+38/60.) = -78.63333333333334
    ncsu_lat = 35.767
    ncsu_lon = -78.633    

    def __init__(self, file_path):
        super().__init__(file_path)
        self.get_variables_min_max(file_path)

    def get_variables_min_max(self, variable_key):
        """

        :param variable_key: The netcdf (filename) key we need to target
        :return: list longitude coordinates
        """
        #IMPACTS_sounding_20200220_1649_NCSU.nc
        tkn = variable_key.split('/')[-1].split('_')
        start_time = datetime.strptime(f'{tkn[2]}{tkn[3][0:4]}',"%Y%m%d%H%M")

        self.minTime, self.maxTime = [start_time, start_time + timedelta(seconds=3600)]
        self.max_lat, self.min_lat, self.max_lon, self.min_lon = \
         [self.ncsu_lat+0.01, self.ncsu_lat-0.01, self.ncsu_lon+0.01, self.ncsu_lon-0.01]

        return

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry from an ASCII  file
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the KML not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in
                                    [self.max_lat, self.min_lat, self.max_lon, self.min_lon]]
        return [self.convert_360_to_180(west), north, self.convert_360_to_180(east), south]

    def get_temporal(self, time_variable_key='time', units_variable='units', scale_factor=1.0, offset=0,
                     date_format='%Y-%m-%dT%H:%M:%SZ'):
        """
        :param time_variable_key: The KML variable we need to target
        :param units_variable: The KML variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :param date_format IF specified the return type will be a string type
        :return:
        """

        start_date = self.minTime.strftime(date_format)
        stop_date = self.maxTime.strftime(date_format)
        return start_date, stop_date

#        data = metadata.get_metadata(ds_short_name=ds_short_name, time_variable_key=time_variable_key,
#                                     lon_variable_key=lon_variable_key, lat_variable_key=lat_variable_key,
#                                     time_units=time_units, format=format, version=version)

    def get_metadata(self, ds_short_name, time_variable_key='time', lon_variable_key='lon',
                     lat_variable_key='lat', time_units='units', format='netCDF-4', version='01'):
        """
        extracts time and geometry data from the granule
        :param ds_short_name:
        :param time_variable_key:
        :param lon_variable_key:
        :param lat_variable_key:
        :param time_units:
        :param format:
        :return:
        """

        start_date, stop_date = self.get_temporal(time_variable_key=time_variable_key,
                                                                              units_variable=time_units,
                                                                              date_format='%Y-%m-%dT%H:%M:%SZ')
        data = dict()
        data['ShortName'] = ds_short_name
        data['GranuleUR'] = self.file_path.split('/')[-1]
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        geometry_list = self.get_wnes_geometry()

        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(str(x) for x in geometry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = format
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting Ncsusndimpacts Metadata')
    file_path = "/home/lwang13/GHRCCLOUD/granule-metadata-extractor/test/fixtures/IMPACTS_sounding_20200220_1649_NCSU.nc" 
    exnet = ExtractNcsusndimpactsMetadata(file_path)
    metada = exnet.get_metadata("test")
    print(metada)
