from ..src.extract_netcdf_metadata import ExtractNetCDFMetadata
from datetime import datetime, timedelta
from netCDF4 import Dataset
import numpy as np

class ExtractUalbparsimpactsMetadata(ExtractNetCDFMetadata):
    """
    A class to extract Ualbparsimpacts netCDF-4
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
        self.format = 'netCDF-4'

        self.get_variables_min_max()

    def get_variables_min_max(self):
        """
        Extract metadata information from netcdf file
        """
        datafile = Dataset(self.file_path)
        lats = datafile['latitude'][0].item()
        lons = datafile['longitude'][0].item()*(-1.) #units: deg west
        sec = np.array(datafile['time'][:])
        #if 2020:     units: seconds since 2020-02-28 00:00:00
        #if 2022:     units: seconds since 2022-01-11
        #             or
        #             units: seconds since 2022-01-10T16:19:20 
        tkn = datafile['time'].units.split(' ')
        ref_time_str = 'T'.join(tkn[-2:]) #i.e., '2020-01-30T00:00:00'
        ref_time = datetime.strptime(ref_time_str,'%Y-%m-%dT%H:%M:%S')

        self.start_time, self.end_time = [ref_time+timedelta(seconds=sec.min().item()),
                                          ref_time+timedelta(seconds=sec.max().item())]
        self.north, self.south, self.east, self.west = [lats+0.01,
                                                        lats-0.01,
                                                        lons+0.01,
                                                        lons-0.01]
        datafile.close()

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
