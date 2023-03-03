from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os
from datetime import datetime, timedelta
import numpy as np
from netCDF4 import Dataset
import math

class ExtractModiscpexMetadata(ExtractNetCDFMetadata):
    """
    A class to extract modiscpex
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


        #extract time info from file name
        tkn = self.file_path.split('/')[-1].split('.nc')[0].split('_')
        minTime = datetime.strptime(''.join(tkn[3:5]),'A%Y%j%H%M') #i.e., A20171290600
        maxTime = datetime.strptime(tkn[-1],'%Y%j%H%M%S') #i.e., 2017135133252 

        fp = Dataset(self.file_path)
        #CPEX_MYD06_L2_A2017129_0600_006_2017135133252.nc

        lat0 = np.array(fp['Latitude']).ravel()  #missing/fill value = -999.9
        lon0 = np.array(fp['Longitude']).ravel() #missing/fill value = -999.9

        lat = [x for x in lat0 if x != -999.9]
        lon = [x for x in lon0 if x != -999.9]

        maxlat = np.nanmax(lat)
        minlat = np.nanmin(lat)
        maxlon = np.nanmax(lon)
        minlon = np.nanmin(lon)

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
