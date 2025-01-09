from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
from datetime import datetime, timedelta
from netCDF4 import Dataset
from pyhdf.HDF import *
from pyhdf.V import *
from pyhdf.SD import SD, SDC
import numpy as np


class ExtractAMSUAMetadata(ExtractNetCDFMetadata):
    """
    A class to extract Namdrop_raw
    """
    start_time = None
    end_time = None
    north = None
    east = None
    south = None
    west = None
    format = None

    def __init__(self, file_path):
        # super().__init__(file_path)
        self.file_path = file_path
        self.choose_file_type()

    def choose_file_type(self):
        """
        Chooses which method to use to extract metadata info of file
        :return:
        :rtype:
        """
        if '.nc' in self.file_path:
            self.format = 'netCDF-4'
            self.extract_netcdf()
        else:
            self.format = 'HDF-EOS-4'
            self.extract_hdf()

    def extract_netcdf(self):
        """
        Extract metadata information from netcdf file
        """
        dataset = Dataset(self.file_path)
        collection_start = datetime(1993, 1, 1, 0, 0)
        self.start_time, self.end_time = [collection_start + x for x in
                                          [timedelta(seconds=np.min(dataset.variables['time'])),
                                           timedelta(seconds=np.max(dataset.variables['time']))]]
        self.south, self.north, self.west, self.east = [np.min(dataset.variables['latitude']),
                                                        np.max(dataset.variables['latitude']),
                                                        np.min(dataset.variables['longitude']),
                                                        np.max(dataset.variables['longitude'])]

    def extract_hdf(self):
        """
        Extracts metadata information of hdf file
        """
        hdf_file = HDF(self.file_path, HC.READ)
        time_list = hdf_file.vstart().attach('Time')[:]
        collection_start = datetime(1993, 1, 1, 0, 0)
        self.start_time = collection_start + timedelta(seconds=np.min(time_list))
        self.end_time = collection_start + timedelta(seconds=np.max(time_list))
        hdf_file.close()

        in_file = SD(self.file_path, SDC.READ)
        lat_data, lon_data = [in_file.select('Latitude').get(), in_file.select('Longitude').get()]
        self.south, self.north, self.west, self.east = [np.min(lat_data), np.max(lat_data),
                                                        np.min(lon_data), np.max(lon_data)]

    def get_wnes_geometry(self, scale_factor=1.0, offset=0):
        """
        Extract the geometry from a netCDF file
        :param nc_data: netCDF data
        :param timestamp:  The NetCDF variable we need to target
        :param scale_factor: In case it is not CF compliant we will need scale factor
        :param offset: data offset if the netCDF not CF compliant
        :return: list of bounding box coordinates [west, north, east, south]
        """
        north, south, east, west = [round((x * scale_factor) + offset, 3) for x in
                                    [self.north, self.south, self.east, self.west]]
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
        start_date = self.start_time.strftime(date_format)
        stop_date = self.end_time.strftime(date_format)
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
        start_date, stop_date = self.get_temporal(time_variable_key='lon',
                                                  units_variable='time',
                                                  date_format='%Y-%m-%dT%H:%M:%SZ')
        data = dict()
        data['ShortName'] = ds_short_name
        data['GranuleUR'] = self.file_path.split('/')[-1]
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        wnes_list = self.get_wnes_geometry()
        # Invalid data values may occur. To avoid NRT interruptions, invalid values, result in the
        # ENTIRE lat/lon metadata being overwritten to collection lat/lon values. This also allows
        # DMG visibility to granules which may contain invalid values.
        if (any( wnes_list[0] < -180, wnes_list[1] > 90, wnes_list[3] > 180, wnes_list[4] < -90 )):
            wnes_list = [-180, 90, 180, -90]

        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in wnes_list)

        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = self.format
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting AMSUA Metadata')
