from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os
from datetime import datetime, timedelta
import numpy as np
from netCDF4 import Dataset
import math

class ExtractAamhcpexMetadata(ExtractNetCDFMetadata):
    """
    A class to extract aamhcpex
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path
        #these are needed to metadata extractor
        self.fileformat = 'netCDF-4'

        # extracting time and space metadata from data file
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max()

    def get_variables_min_max(self):

        #extract time info from file name
        tkn= self.file_path.split('/')[-1].split('_')
        minTime = datetime.strptime(''.join([tkn[8],tkn[9]]),'D%y%jS%H%M')
        maxTime = datetime.strptime(''.join([tkn[8],tkn[10]]),'D%y%jE%H%M')

        #CPEX_NOAA18_NPR_MIRS_V7_SND_AAMH_NN_D17193_S2333_E0128_B6258991_NS.nc
        #CPEX_METOPA_NPR_MIRS_V7_SND_AAMH_M2_D17196_S2240_E0023_B5572324_NS.nc
        if maxTime < minTime:
           maxTime = maxTime + timedelta(days = 1)


        fp = Dataset(self.file_path)
        lat = np.array(fp['Latitude'])
        lon = np.array(fp['Longitude'])

        lon_0to360 = lon
        lon_0to360[lon_0to360 < 0] = lon_0to360[lon_0to360 < 0]+360.

        maxlat = np.nanmax(lat)
        minlat = np.nanmin(lat)
        maxlon = np.nanmax(lon_0to360)
        minlon = np.nanmin(lon_0to360)

        #convert maxlon, minLon from 0_to_360 to -180_to_180
        maxlon = maxlon - 360. if maxlon > 180. else maxlon
        minlon = minlon - 360. if minlon > 180. else minlon

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
