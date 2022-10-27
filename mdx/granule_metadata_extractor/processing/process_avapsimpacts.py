from ..src.extract_ascii_metadata import ExtractASCIIMetadata
from datetime import datetime, timedelta
import os
import re
from netCDF4 import Dataset


class ExtractAvapsimpactsMetadata(ExtractASCIIMetadata):
    """
    A class to extract Avapsimpacts granule metadata
    """
    start_time = datetime(2100, 1, 1)
    end_time = datetime(1900, 1, 1)
    north = -90.0
    south = 90.0
    east = -180.0
    west = 180.0

    def __init__(self, file_path):
        super().__init__(file_path)
        self.file_path = file_path
        self.filename = os.path.basename(self.file_path)
        if file_path.endswith('.ict'):
            self.fileformat = "ASCII"
        else: #.nc
            self.fileformat = "netCDF-3"
        self.get_variables_min_max()

    def get_variables_min_max(self, **kwargs):
        """
        Extracts temporal metadata and assign spatial metadata for avapsimpacts granules
        """
        if self.file_path.endswith('.ict'): #ASCII file
           lat = []
           lon = []
           dt = []
           dt_base = datetime.strptime(self.filename.split('/')[-1].split('_')[3][0:8], '%Y%m%d')

           with open(self.file_path, 'rb') as fp:
                lines = fp.readlines()

           for line in lines:
               #decoding
               line = line.decode('utf-8',errors='ignore')
               matches = re.search(r'^([\d.-]*),(([\d.-]*),){7}([\d.-]*),.*$', line)
               if matches and all([(x != "-9999") for x in [matches[3], matches[4]]]):
                  lat.append(float(matches[3]))
                  lon.append(float(matches[4]))
                  dt.append(float(matches[1]))

           self.start_time = dt_base + timedelta(seconds=min(dt))
           self.end_time = dt_base + timedelta(seconds=max(dt))
           self.north, self.south, self.east, self.west = [max(lat), min(lat), max(lon), min(lon)]
        else: #netCDF-3 file
           data = Dataset(self.file_path)
           reftime_str = 'T'.join(data['time'].units.split()[2:4]) #i.e., '2022-01-06T17:05:09'
           dt_base = datetime.strptime(reftime_str,'%Y-%m-%dT%H:%M:%S')
           dt0 = data['time'][:].flatten() #seconds since reftime
           lat0 = data['lat'][:].flatten()
           lon0 = data['lon'][:].flatten()

           #get indices for vaid lat and lon values
           idx = [i for i in range(0,len(lat0)) if lat0.mask[i] == False and lon0.mask[i] == False]
           dt = dt0[idx]
           lat = lat0[idx]
           lon = lon0[idx]

           self.start_time = dt_base+timedelta(seconds=dt.min())
           self.end_time = dt_base+timedelta(seconds=dt.max())
           self.north, self.south, self.east, self.west = [lat.max(), lat.min(), lon.max(), lon.min()]

    def get_wnes_geometry(self, scale_factor=1.0, offset=0, **kwargs):
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

    def get_metadata(self, ds_short_name, format='Binary', version='01', **kwargs):
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
        data['GranuleUR'] = self.filename
        data['BeginningDateTime'], data['EndingDateTime'] = start_date, stop_date

        gemetry_list = self.get_wnes_geometry()

        data['WestBoundingCoordinate'], data['NorthBoundingCoordinate'], \
        data['EastBoundingCoordinate'], data['SouthBoundingCoordinate'] = list(
            str(x) for x in gemetry_list)
        data['SizeMBDataGranule'] = str(round(self.get_file_size_megabytes(), 2))
        data['checksum'] = self.get_checksum()
        data['DataFormat'] = self.fileformat
        data['VersionId'] = version
        return data


if __name__ == '__main__':
    print('Extracting Avapsimpacts Metadata')
