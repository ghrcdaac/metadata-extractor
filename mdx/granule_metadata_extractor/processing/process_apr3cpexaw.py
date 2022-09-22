from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os
from datetime import datetime, timedelta
import numpy as np
from netCDF4 import Dataset
import math


class ExtractApr3cpexawMetadata(ExtractNetCDFMetadata):
    """
    A class to extract apr3cpexaw
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        #these are needed to metadata extractor
        self.fileformat = 'HDF-5'

        # extracting time and space metadata from .mat file
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max()

    def get_variables_min_max(self):

        fp = Dataset(self.file_path)

        utc_sec0 = np.array(fp['lores/scantime']).ravel() #Seconds since 1,1,1970; flatten 2d to 1d
        lat0 = np.array(fp['lores/lat']).ravel()  #missing/fill value = 0.0
        lon0 = np.array(fp['lores/lon']).ravel() #missing/fill value = 0.0

        utc_sec0_nan = [x for x in utc_sec0 if math.isnan(x)]
        if len(utc_sec0_nan) > 0:
           #remove records with scantime == float('nan')
           num_rec = len(utc_sec0)
           utc_sec =[utc_sec0[i] for i in range(0,num_rec) if not np.isnan(utc_sec0[i])]
           lat = [lat0[i] for i in range(0,num_rec) if not np.isnan(utc_sec0[i])]
           lon = [lon0[i] for i in range(0,num_rec) if not np.isnan(utc_sec0[i])]
        else: #if no 'nan' values
           utc_sec = utc_sec0
           lat = lat0
           lon = lon0

        timestamps = [datetime(1970,1,1) + timedelta(seconds=x) for x in utc_sec]

        #mask out 0.0 values if any
        lat = np.ma.masked_equal(lat, 0.0)
        lon = np.ma.masked_equal(lon, 0.0)

        minTime = min(timestamps)
        maxTime = max(timestamps)
        maxlat = lat.max()
        minlat = lat.min()
        maxlon = lon.max()
        minlon = lon.min()

        fp.close()

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

    def get_metadata(self, ds_short_name, format='HDF-5', version='1', **kwargs):
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
