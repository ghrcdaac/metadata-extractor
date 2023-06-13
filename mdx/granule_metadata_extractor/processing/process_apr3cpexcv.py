from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os
from datetime import datetime, timedelta
import numpy as np
from netCDF4 import Dataset
import math

class ExtractApr3cpexcvMetadata(ExtractNetCDFMetadata):
    """
    A class to extract apr3cpexcv
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        #these are needed to metadata extractor
        self.fileformat = 'netCDF-4'

        # extracting time and space metadata from .nc file
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max()

    def get_variables_min_max(self):

        fp = Dataset(self.file_path)
        utc_lores_ref = datetime(1970,1,1)
        utc_lores = np.array(fp['lores_scantime']) #utc seconds, 2-d numpy array in 'APR3' file, 1-d in 'APR3nad' file
        minTime = utc_lores_ref + timedelta(seconds=np.nanmin(utc_lores))
        maxTime = utc_lores_ref + timedelta(seconds=np.nanmax(utc_lores))
        
        north,south,east,west = [-90.,90.,-180.,180.]
        if '_APR3_' in self.file_path:
           lat_lores = np.array(fp['lores_lat3D']) #3-d numpy arrays
           lon_lores = np.array(fp['lores_lon3D']) #3-d numpy arrays

           for i in range(0,utc_lores.shape[0]):
               for j in range(1,utc_lores.shape[1]):
                   if not math.isnan(utc_lores[i,j]):
                      llat = lat_lores[i,j,:].ravel()
                      llon = lon_lores[i,j,:].ravel()

                      #mask out 0.0 values if any
                      llat = np.ma.masked_equal(llat, 0.0)
                      llon = np.ma.masked_equal(llon, 0.0)

                      if not np.isnan(llat).all() and not np.isnan(llon).all():
                         north = max(north,np.nanmax(llat))
                         south = min(south,np.nanmin(llat))
                         east = max(east,np.nanmax(llon))
                         west = min(west,np.nanmin(llon))
        else: #'_APR3nad_'
           lat_lores = np.array(fp['lores_sfc_lat']) #2-d numpy arrays
           lon_lores = np.array(fp['lores_sfc_lon']) #2-d numpy arrays

           for i in range(0,utc_lores.shape[0]):
               if not math.isnan(utc_lores[i]):
                  llat = lat_lores[i,:].ravel()
                  llon = lon_lores[i,:].ravel()

                  #mask out 0.0 values if any
                  llat = np.ma.masked_equal(llat, 0.0)
                  llon = np.ma.masked_equal(llon, 0.0)

                  if not np.isnan(llat).all() and not np.isnan(llon).all():
                     north = max(north,np.nanmax(llat))
                     south = min(south,np.nanmin(llat))
                     east = max(east,np.nanmax(llon))
                     west = min(west,np.nanmin(llon))

        maxlat = north 
        minlat = south 
        maxlon = east 
        minlon = west 

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
        data['DataFormat'] = self.fileformat
        data['VersionId'] = version
        return data
