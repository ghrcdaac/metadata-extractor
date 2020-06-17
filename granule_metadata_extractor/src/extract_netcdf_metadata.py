from .metadata_extractor import MetadataExtractor
from netCDF4 import Dataset
import numpy as np
import re
from datetime import datetime
from dateutil.parser import parse
from hashlib import md5

class ExtractNetCDFMetadata(MetadataExtractor):
    """
    Class to extract netCDF metadata
    """
    def __init__(self, file_path):
        super().__init__(file_path)
        self.nc = Dataset(self.file_path)
        self.variables = self.nc.variables
        

    def __get_min_max(self, np_array):
        """
        Get the min and the max from a numpy array
        :param np_array: numpy array
        :return: min, max
        """
        # Return min and max and ignoring the nan
        # if True in np.isnan(np_array):
        #     return np.nanmin(np_array), np.nanmax(np_array)
        return np.nanmin(np_array), np.nanmax(np_array)

    def get_variables_min_max(self, variable_key):
        """

        :param c_data: netCDF data
        :param variable_key: The NetCDF key we need to target
        :return: list longitude coordinates
        """
        return self.__get_min_max(self.variables[variable_key][0:].data)


    def get_wnes_geometry(self, variable_lon_key='lon', variable_lat_key='lat', scale_factor=1.0, offset=0):
        """
        Extract the geometry from a netCDF file
        :param nc_data: netCDF data
        :param variable_lon_key:  The NetCDF variable we need to target
        :param variable_lat_key:  The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """

        south, north = [round((x * scale_factor) + offset, 3) for x in self.get_variables_min_max(variable_lat_key)]
        west, east = [round((x * scale_factor) + offset, 3) for x in self.get_variables_min_max(variable_lon_key)]

        return [self.convert_360_to_180(west), north, self.convert_360_to_180(east), south]



    def get_temporal(self, time_variable_key='time', units_variable='units',  scale_factor=1.0, offset=0,
                     date_format = '%Y-%m-%dT%H:%M:%SZ'):
        """
        :param time_variable_key: The NetCDF variable we need to target
        :param units_variable: The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :param date_format IF specified the return type will be a string type
        :return:
        """
        if time_variable_key == 'void':
            start_date = datetime.strptime(str(self.nc.__dict__['begin_time']), '%Y-%m-%dT%H:%M:%SZ')
            stop_date = datetime.strptime(str(self.nc.__dict__['end_time']), '%Y-%m-%dT%H:%M:%SZ')
        else:
            reg_ex = r'(.*) since (.*)'
            unit = getattr(self.variables[time_variable_key], units_variable)
            group = re.search(reg_ex, unit).group
            min_data, max_data = self.get_variables_min_max(time_variable_key)
            args_min = {group(1): (scale_factor*min_data) + offset}
            args_max = {group(1): (scale_factor * max_data) + offset}
            start_date = self.get_updated_date(group(2), **args_min)
            stop_date = self.get_updated_date(group(2), **args_max)
        if date_format:
            start_date, stop_date = start_date.strftime(date_format), stop_date.strftime(date_format)
        return start_date, stop_date

    def get_metadata(self, ds_short_name, time_variable_key='time', lon_variable_key='lon',
                     lat_variable_key='lat', time_units='units', format='netCDF-4', version='01'):
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
        data['ShortName'] = ds_short_name
        data['GranuleUR'] = self.file_path.split('/')[-1]
        data['BeginningDateTime'], data['EndingDateTime'] = self.get_temporal(time_variable_key=time_variable_key,
                                                                              units_variable=time_units,
                                                                              date_format='%Y-%m-%dT%H:%M:%SZ')

        gemetry_list =self.get_wnes_geometry(lon_variable_key, lat_variable_key)

        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(str(x) for x in gemetry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = format
        data['VersionId'] = version
        return data

if __name__ == '__main__':
    print("Dependencies works")

    file_path = "/home/amarouane/Downloads/GOESR_CRS_L1B_20170411_v0.nc"
    exnet = ExtractNetCDFMetadata(file_path)
    # # print(exnet.get_checksum(file_path))
    # # print(exnet.get_file_size(file_path))
    # target_variable = 'time'
    # units_variable = 'units'
    # # uni = exnet.get_units(nc, target_variable)
    # # print(uni)
    # #
    # #
    # start, stop = exnet.get_temporal(units_variable=units_variable)
    # print(start, stop)

    target_variable_lat1 = 'lat'
    target_variable_lon1 = 'lon'
    K = exnet.get_wnes_geometry(target_variable_lon1, target_variable_lat1)

    print(K)

    #file_path = "local/HAMSR_L1B_20130925T051206_20130925T205025_v01.nc"
    #exnet = ExtractNetCDFMetadata(file_path)
    # print(exnet.get_checksum(file_path))
    # print(exnet.get_file_size(file_path))

    # uni = exnet.get_units(nc, target_variable)
    # print(uni)
    #
    #
    # start, stop = exnet.get_temporal()
    # print(start, stop)
    #
    # # target_variable_lat1 = {'units': 'degrees_north'}
    # # target_variable_lon1 = {'units': 'degrees_east'}
    # K = exnet.get_wnes_geometry()
    # #
    # print(K)
