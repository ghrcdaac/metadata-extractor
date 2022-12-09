from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os
from datetime import datetime, timedelta
import numpy as np
from netCDF4 import Dataset
import math

class ExtractGoescpexMetadata(ExtractNetCDFMetadata):
    """
    A class to extract goescpex
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        #these are needed to metadata extractor
        self.fileformat = 'netCDF-3'

        # extracting time and space metadata from .mat file
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max()

    def get_variables_min_max(self):

        #Read date/time info from filename
        #Sample file: CPEX_GOES_20170610_0015_g13.nc
        data = Dataset(self.file_path)
        tkn = self.file_path.split('/')[-1].split('.nc')[0].split('_')
        utc_str = ''.join(tkn[2:4]) #i.e., 201706100015
        utc_ref = datetime.strptime(tkn[2],'%Y%m%d')
        utc_sec = data.start_time+data.time_adjust+data.scan_time[:]
        minTime = utc_ref + timedelta(seconds=utc_sec.min())
        maxTime = utc_ref + timedelta(seconds=utc_sec.max())

        lat_ch1 = data['latitude_ch1'][:]
        lon_ch1 = data['longitude_ch1'][:]
        lat_ch4 = data['latitude_ch4'][:]
        lon_ch4 = data['longitude_ch4'][:]
        lat_ch6 = data['latitude_ch6'][:]
        lon_ch6 = data['longitude_ch6'][:]
        maxlat = max([lat_ch1.max(),lat_ch4.max(),lat_ch6.max()])
        minlat = min([lat_ch1.min(),lat_ch4.min(),lat_ch6.min()])
        maxlon = max([lon_ch1.max(),lon_ch4.max(),lon_ch6.max()])
        minlon = min([lon_ch1.min(),lon_ch4.min(),lon_ch6.min()])

        data.close()

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

    def get_metadata(self, ds_short_name, format='netCDF-3', version='1', **kwargs):
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
