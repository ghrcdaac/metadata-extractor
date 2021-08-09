from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
from datetime import datetime, timedelta
from netCDF4 import Dataset
import numpy as np

class ExtractUalbmrr2impactsMetadata(ExtractNetCDFMetadata):
    """
    A class to extract Ualbmrr2impacts netCDF-3
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
        self.format = 'netCDF-3'

        self.get_variables_min_max()

    def get_variables_min_max(self):
        """
        Extract metadata information from netcdf file
        """
        nc = Dataset(self.file_path, 'r')
        lat = nc.variables['lat'][:]
        lon = nc.variables['lon'][:]
        tm = nc.variables['time'][:]
        self.north, self.south, self.east, self.west = [np.max(lat)+0.06/111.325, np.min(lat)-0.06/111.325,
                                                        np.max(lon)+0.06/111.325, np.min(lon)-0.06/111.325]

        self.start_time = datetime(1970,1,1) + timedelta(seconds=float(np.min(tm)))
        self.end_time = datetime(1970,1,1) + timedelta(seconds=float(np.max(tm)))

        nc.close()

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
                     lat_variable_key='lat', time_units='units', format='netCDF-3', version='01'):
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

        gemetry_list = self.get_wnes_geometry()

        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in gemetry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = self.format
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting ualbparsimpacts Metadata')
