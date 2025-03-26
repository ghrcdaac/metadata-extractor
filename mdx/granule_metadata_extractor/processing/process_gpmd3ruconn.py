from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
import os, re
from datetime import datetime, timedelta
from math import radians, degrees, sin, cos, asin, acos, sqrt
from netCDF4 import Dataset
import numpy as np

class ExtractGpmd3ruconnMetadata(ExtractNetCDFMetadata):
    """
    A class to extract sbuskylerimpacts 
    """

    def __init__(self, file_path):
        #super().__init__(file_path)
        self.file_path = file_path

        # extracting time and space metadata from .nc file
        nc = Dataset(self.file_path)
        self.fileformat = 'netCDF-4'
        [self.minTime, self.maxTime, self.SLat, self.NLat, self.WLon, self.ELon] = \
                        self.get_variables_min_max_nc(nc)
        nc.close()


    def get_variables_min_max_nc(self,data):
        #Calculate D3R radar range in km
        gate_num = data.dimensions['Gate'].size  #266 gates
        gate_width = np.array(data['GateWidth'][:]).max() #Unit: millimeter
        radar_range = gate_width * gate_num *1.e-6 # Units: km

        #Earth radius ~ 6371 km
        mlat = radians(data.Latitude)
        mlon = radians(data.Longitude)
        plat = mlat
        dlon = degrees(acos((cos(radar_range/6371)-sin(mlat)*sin(plat))/cos(mlat)/cos(plat)))
        dlat = degrees(radar_range/6371.)
        maxlat, minlat, maxlon, minlon = [data.Latitude+dlat, data.Latitude-dlat, 
                                          data.Longitude+dlon, data.Longitude-dlon]

        #Time: unit: seconds since 1970-01-01 00:00:00
        sec = np.array(data['Time'][:])
        minTime = datetime(1970,1,1) + timedelta(seconds=int(min(sec)))
        maxTime = datetime(1970,1,1) + timedelta(seconds=int(max(sec)))

        return minTime, maxTime, minlat, maxlat, minlon, maxlon


    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
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
